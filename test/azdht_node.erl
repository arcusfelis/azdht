%% RPC-node.
-module(azdht_node).
-export([my_contact/1,
         ping/2,
         find_node/3,
         find_value/3,
         announce/5]).

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

%% @doc Call `Contact' from `Node' to lookup for `NodeID'.
find_node(Node, Contact, NodeID) ->
    case rpc:call(Node, azdht_net, find_node, [Contact, NodeID], 5000) of
        {badrpc, Reason} -> erlang:error({badrpc, Reason});
        Result -> Result
    end.

%% @doc Call `Contact' from `Node' to lookup for `EncodedKey'.
find_value(Node, Contact, EncodedKey) ->
    case rpc:call(Node, azdht_net, find_value, [Contact, EncodedKey], 5000) of
        {badrpc, Reason} -> erlang:error({badrpc, Reason});
        Result -> Result
    end.

%% @doc Announce yourself on port `PortBT' to `Contact' from `Node'.
announce(Node, Contact, SpoofID, EncodedKey, PortBT) ->
    Args = [Contact, SpoofID, EncodedKey, PortBT],
    case rpc:call(Node, azdht_net, announce, Args, 5000) of
        {badrpc, Reason} -> erlang:error({badrpc, Reason});
        Result -> Result
    end.
