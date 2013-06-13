%% @author Jesper Louis Andersen <jesper.louis.andersen@gmail.com>
%% @doc Main azdht supervisor
%% <p>This Supervisor is the top-level supervisor of azdht. It
%% starts well over 10 processes when it is initially started. It
%% will restart parts of azdht, should they suddenly die
%% unexpectedly, but it is assumed that many of these processes do not die.</p>
%% @end
-module(azdht_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(APP, azdht).

%% ====================================================================

%% @doc Start the supervisor
%% @end
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    ListenPort = get_required_app_env(listen_port),
    ExternalIP = get_required_app_env(external_ip),
    ListenIP   = get_app_env(listen_ip, all),
    StateFile  = get_app_env(state_filename),
    start_link(ListenIP, ListenPort, ExternalIP, StateFile).

start_link(ListenIP, ListenPort, ExternalIP, StateFile) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE,
                          [ListenIP, ListenPort, ExternalIP, StateFile]).


%% ====================================================================

%% @private
init([ListenIP, ListenPort, ExternalIP, StateFile]) ->
    MyContact = azdht:my_contact(ExternalIP, ListenPort),
    Net = {azdht_net,
           {azdht_net, start_link, [ListenIP, ListenPort, ExternalIP]},
            permanent, 5000, worker, [azdht_net]},
    Router = {azdht_router,
           {azdht_router, start_link, [MyContact, StateFile]},
            permanent, 5000, worker, [azdht_router]},
    DB = {azdht_db,
           {azdht_db, start_link, []},
            permanent, 5000, worker, [azdht_db]},
    Cache = {azdht_spoof_cache,
           {azdht_spoof_cache, start_link, []},
            permanent, 5000, worker, [azdht_spoof_cache]},

    {ok, {{one_for_all, 3, 60}, [Cache, Net, Router, DB]}}.


%% ====================================================================
get_app_env(Key) ->
    get_app_env(Key, undefined).

get_app_env(Key, Def) ->
    case application:get_env(?APP, Key) of
        {ok, Value} -> Value;
        undefined -> Def
    end.

get_required_app_env(Key) ->
    case application:get_env(?APP, Key) of
        {ok, Value} -> Value;
        undefined -> erlang:error({undefined_env_var, Key})
    end.
