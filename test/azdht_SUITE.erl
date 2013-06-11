-module(azdht_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("azdht/include/azdht.hrl").

-export([suite/0,
         all/0,
         groups/0,
	     init_per_suite/1,
	     init_per_group/2,
	     init_per_testcase/2,
         end_per_suite/1,
         end_per_group/2,
         end_per_testcase/2]).

-export([ping/0,
         ping/1,
         find_node/0,
         find_node/1]).


suite() ->
    application:start(crypto),
    [{timetrap, {minutes, 5}}].

%% Setup/Teardown
%% ----------------------------------------------------------------------
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    %% Start slave nodes.
    {ok, Node1} = test_server:start_node(node1, slave, []),
    {ok, Node2} = test_server:start_node(node2, slave, []),
    %% Run logger on the slave nodes
    [prepare_node(Node, Name)
     || {Node, Name} <- [{Node1, "N1"}, {Node2, "N2"}]],
    [{node1, Node1},
     {node2, Node2} | Config].



end_per_suite(Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),
    test_server:stop_node(Node1),
    test_server:stop_node(Node2),
    ok.


init_per_testcase(ping, Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),
    Node1Conf = node1_configuration(Node1),
    Node2Conf = node2_configuration(Node2),
    start_app(Node1, Node1Conf),
    start_app(Node2, Node2Conf),
    Config;

init_per_testcase(find_node, Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),
    Node1Conf = node1_configuration(Node1),
    Node2Conf = node2_configuration(Node2),
    start_app(Node1, Node1Conf),
    start_app(Node2, Node2Conf),
    Config.


end_per_testcase(ping, Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),
    stop_app(Node1),
    stop_app(Node2),
    ok;

end_per_testcase(find_node, Config) ->
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),
    stop_app(Node1),
    stop_app(Node2),
    ok.

%% Configuration
%% ----------------------------------------------------------------------

node1_configuration(Dir) ->
    [{listen_ip, {127,0,0,2}},
     {external_ip, {127,0,0,2}},
     {listen_port, 43301 }
    | ct:get_config(common_conf)].

node2_configuration(Dir) ->
    [{listen_ip, {127,0,0,3}},
     {external_ip, {127,0,0,3}},
     {listen_port, 43302 }
    | ct:get_config(common_conf)].


%% Tests
%% ----------------------------------------------------------------------
groups() ->
    Tests = [ping, find_node],
%   [{main_group, [shuffle], Tests}].
    [{main_group, [], Tests}].

all() ->
    [{group, main_group}].

ping() ->
    [{require, common_conf, azdht_common_config}].

ping(Config) ->
    io:format("~n======START PING TEST CASE======~n", []),
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),
    Contact2 = azdht_node:my_contact(Node2),
    %% Ping Node2 from Node1.
    PingResult = azdht_node:ping(Node1, Contact2),
    ct:pal("PingResult ~p", [PingResult]),
    case PingResult of
        {ok, #ping_reply{}} -> ok
    end.


find_node() ->
    [{require, common_conf, azdht_common_config}].

find_node(Config) ->
    io:format("~n======START FIND NODE TEST CASE======~n", []),
    Node1 = ?config(node1, Config),
    Node2 = ?config(node2, Config),
    Contact1 = azdht_node:my_contact(Node1),
    Contact2 = azdht_node:my_contact(Node2),
    %% Just ping Node2 from Node1.
    PingResult = azdht_node:ping(Node1, Contact2),
    ct:pal("PingResult ~p", [PingResult]),
    %% Request Node2 from Node1 to identify yourself.
    FindNodeResult = azdht_node:find_node(Node1, Contact2, azdht:node_id(Contact1)),
    ct:pal("FindNodeResult ~p", [FindNodeResult]),
    case FindNodeResult of
        {ok, #find_node_reply{contacts=[]}} -> ok
    end.



%% Helpers
%% ----------------------------------------------------------------------

prepare_node(Node, NodeName) ->
    io:format("Prepare node ~p.~n", [Node]),
    rpc:call(Node, code, set_path, [code:get_path()]),
    true = rpc:call(Node, erlang, unregister, [user]),
    IOProxy = spawn(Node, spawn_io_proxy()),
    true = rpc:call(Node, erlang, register, [user, IOProxy]),
    Handlers = lager_handlers(NodeName),
    ok = rpc:call(Node, application, load, [lager]),
    ok = rpc:call(Node, application, set_env, [lager, handlers, Handlers]),
    ok = rpc:call(Node, application, start, [lager]),
    ok.

spawn_io_proxy() ->
    User = group_leader(),
    fun() -> io_proxy(User) end.
    
io_proxy(Pid) ->
    receive
        Mess -> Pid ! Mess, io_proxy(Pid)
    end.

lager_handlers(NodeName) ->
%   [Node|_] = string:tokens(atom_to_list(NodeName), "@"),
    Format = [NodeName, "> ", "[", time, "] [",severity,"] ",
              {pid, [pid, " "], ""}, {module, [module, ":", line, " "], ""},
              message, "\n"],
    [{lager_console_backend, [debug, {lager_default_formatter, Format}]}].


stop_app(Node) ->
    ok = rpc:call(Node, azdht_app, stop, []).

start_app(Node, AppConfig) ->
    ok = rpc:call(Node, azdht_app, start, [AppConfig]).
