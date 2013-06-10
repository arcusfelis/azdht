%% RPC-node.
-module(azdht_node).
-export([my_contact/1,
         ping/2]).

my_contact(Node) ->
    case rpc:call(Node, azdht_net, my_contact, [], 5000) of
        {badrpc, Reason} -> erlang:error({badrpc, Reason});
        Result -> Result
    end.

%% @doc Ping `Contact' from `Node'.
ping(Node, Contact) ->
    case rpc:call(Node, azdht_net, ping, [Contact], 5000) of
        {badrpc, Reason} -> erlang:error({badrpc, Reason});
        Result -> Result
    end.
