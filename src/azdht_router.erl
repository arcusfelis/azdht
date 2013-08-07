%% @author Magnus Klaar <magnus.klaar@sgsstudentbostader.se>
%% @author Uvarov Michael <arcusfelis@gmail.com>
%% @doc A Server for maintaining the the routing table in DHT
%%
%% @todo Document all exported functions.
%%
%% This module implements a server maintaining the
%% DHT routing table. The nodes in the routing table
%% is distributed across a set of buckets. The bucket
%% set is created incrementally based on the local node id.
%%
%% The set of buckets, id ranges, is used to limit
%% the number of nodes in the routing table. The routing
%% table must only contain ?K nodes that fall within the
%% range of each bucket.
%%
%% A node is considered disconnected if it does not respond to
%% a ping query after 10 minutes of inactivity. Inactive nodes
%% are kept in the routing table but are not propagated to
%% neighbouring nodes through responses through find_node
%% and get_peers responses.
%% This allows the node to keep the routing table intact
%% while operating in offline mode. It also allows the node
%% to operate with a partial routing table without exposing
%% inconsitencies to neighboring nodes.
%%
%% A bucket is refreshed once the least recently active node
%% has been inactive for 5 minutes. If a replacement for the
%% least recently active node can't be replaced, the server
%% should wait at least 5 minutes before attempting to find
%% a replacement again.
%%
%% The timeouts (expiry times) in this server is managed
%% using a pair containg the time that a node/bucket was
%% last active and a reference to the currently active timer.
%%
%% The activity time is used to validate the timeout messages
%% sent to the server in those cases where the timer was cancelled
%% inbetween that the timer fired and the server received the timeout
%% message. The activity time is also used to calculate when
%% future timeout should occur.
%%
%% @end
-module(azdht_router).
-behaviour(gen_server).
-export([srv_name/0,
         start_link/2,
         node_id/0,
         safe_insert_node/1,
         safe_insert_nodes/1,
         unsafe_insert_node/1,
         unsafe_insert_nodes/1,
         is_interesting/1,
         closest_to/1,
         closest_to/2,
         closest_to/3,
         log_request_timeout/1,
         log_request_success/1,
         log_request_from/1,
         dump_state/0,
         dump_state/1]).

%% Debug.
-export([node_list/0]).

-include("azdht.hrl").
-define(in_range(IDExpr, MinExpr, MaxExpr),
    ((IDExpr >= MinExpr) and (IDExpr < MaxExpr))).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    my_contact :: contact(),
    node_id :: nodeid(), % My node ID
    buckets=b_new(), % The actual routing table
    node_timers=timer_tree(), % Node activity times and timeout references
    buck_timers=timer_tree(),% Bucker activity times and timeout references
    node_timeout=10*60*1000,  % Default node keepalive timeout
    buck_timeout=5*60*1000,   % Default bucket refresh timeout
    state_file="/tmp/azdht_state"}). % Path to persistent state
%
% The bucket refresh timeout is the amount of time that the
% server will tolerate a node to be disconnected before it
% attempts to refresh the bucket.
%

-include_lib("kernel/include/inet.hrl").

srv_name() ->
    azdht_router.

start_link(MyContact, StateFile) ->
    gen_server:start_link({local, srv_name()},
                          ?MODULE,
                          [MyContact, StateFile], []).


%% @doc Return a this node id as an integer.
%% Node ids are generated in a random manner.
-spec node_id() -> nodeid().
node_id() ->
    gen_server:call(srv_name(), node_id).

%
% Check if a node is available and verify its node id by issuing
% a ping query to it first. This function must be used when we
% want to verify the identify and status of a node.
%
% This function will return {error, timeout} if the node is unreachable
% or has changed identity, false if the node is not interesting or wasnt
% inserted into the routing table, true if the node was interesting and was
% inserted into the routing table.
%
-spec safe_insert_node(contact()) ->
    {'error', 'timeout'} | boolean().
safe_insert_node(Contact) ->
    case is_interesting(Contact) of
        false -> false;
        true ->
            % Since this clause will be reached every time this node
            % receives a query from a node that is interesting, use the
            % unsafe_ping function to avoid repeatedly issuing ping queries
            % to nodes that won't reply to them.
            case unsafe_ping(Contact) of
                pong -> unsafe_insert_node(Contact);
                pang -> {error, timeout};
                _    -> {error, timeout}
        end
    end.

-spec safe_insert_nodes(list(contact())) -> 'ok'.
safe_insert_nodes(Contacts) ->
    [spawn_link(fun() -> safe_insert_node(Contact) end)
    || Contact <- Contacts],
    ok.

%
% Blindly insert a node into the routing table. Use this function when
% inserting a node that was found and successfully queried in a find_node
% or get_peers search.
% This function returns a boolean value to indicate to the caller if the
% node was actually inserted into the routing table or not.
%
-spec unsafe_insert_node(contact()) -> boolean().
unsafe_insert_node(Contact) ->
    _WasInserted = gen_server:call(srv_name(), {insert_node, Contact}).

-spec unsafe_insert_nodes(list(contact())) -> 'ok'.
unsafe_insert_nodes(Contacts) ->
    [spawn_link(fun() -> unsafe_insert_node(Contact) end)
    || Contact <- Contacts],
    ok.

%
% Check if node would fit into the routing table. This
% function is used by the safe_insert_node(s) function
% to avoid issuing ping-queries to every node sending
% this node a query.
%

-spec is_interesting(contact()) -> boolean().
is_interesting(Contact) ->
    gen_server:call(srv_name(), {is_interesting, Contact}).

-spec closest_to(nodeid()) -> list(contact()).
closest_to(NodeID) ->
    closest_to(NodeID, 8).

-spec closest_to(nodeid(), pos_integer()) -> list(contact()).
closest_to(NodeID, NumNodes) ->
    closest_to(NodeID, NumNodes, false).

-spec closest_to(nodeid(), pos_integer(), boolean()) -> list(contact()).
closest_to(NodeID, NumNodes, WithMe)
    when is_boolean(WithMe), is_integer(NumNodes),
         is_binary(NodeID), byte_size(NodeID) =:= 20 ->
    gen_server:call(srv_name(), {closest_to, NodeID, NumNodes, WithMe}).

-spec log_request_timeout(contact()) -> 'ok'.
log_request_timeout(Contact) ->
    Call = {request_timeout, Contact},
    gen_server:call(srv_name(), Call).

-spec log_request_success(contact()) -> 'ok'.
log_request_success(Contact) ->
    Call = {request_success, Contact},
    gen_server:call(srv_name(), Call).

-spec log_request_from(contact()) -> 'ok'.
log_request_from(Contact) ->
    Call = {request_from, Contact},
    gen_server:call(srv_name(), Call).

dump_state() ->
    gen_server:call(srv_name(), dump_state).

dump_state(Filename) ->
    gen_server:call(srv_name(), {dump_state, Filename}).

node_list() ->
    gen_server:call(srv_name(), node_list).

-spec keepalive(contact()) -> 'ok'.
keepalive(Contact) ->
    case safe_ping(Contact) of
        pong  -> log_request_success(Contact);
        pang  -> log_request_timeout(Contact);
        _     -> log_request_timeout(Contact)
    end.

spawn_keepalive(Contact) ->
    spawn_link(fun() -> keepalive(Contact) end).

%
% Issue a ping query to a node, this function should always be used
% when checking if a node that is already a member of the routing table
% is online.
%
-spec safe_ping(contact()) -> pang | nodeid().
safe_ping(Contact) ->
    case azdht_net:ping(Contact) of
       {ok, _} -> pong;
       {error, _} -> pang
    end.

%
% unsafe_ping overrides the behaviour of azdht_net:ping/2 by
% avoiding to issue ping queries to nodes that are unlikely to
% be reachable. If a node has not been queried before, a safe_ping
% will always be performed.
%
% Returns pand, if the node is unreachable.
-spec unsafe_ping(contact()) -> pang | nodeid().
unsafe_ping(Contact) ->
    case ets:member(unreachable_tab(), Contact) of
        true ->
            pang;
        false ->
            case safe_ping(Contact) of
                pang ->
                    RandNode = random_node_tag(),
                    DelSpec = [{{'_', RandNode}, [], [true]}],
                    _ = ets:select_delete(unreachable_tab(), DelSpec),
                    lager:debug("~p is unreachable.",
                                [azdht:compact_contact(Contact)]),
                    ets:insert(unreachable_tab(), {Contact, RandNode}),
                    pang;
                pong ->
                    pong
            end
    end.

%
% Refresh the contents of a bucket by issuing find_node queries to each node
% in the bucket until enough nodes that falls within the range of the bucket
% has been returned to replace the inactive nodes in the bucket.
%
-spec refresh(any(), list(contact()), list(contact())) -> 'ok'.
refresh(Range, Inactive, Active) ->
    % Try to refresh the routing table using the inactive nodes first,
    % If they turn out to be reachable the problem's solved.
    do_refresh(Range, Inactive ++ Active, []).

do_refresh(_, [], _) ->
    ok; % @todo - perform a find_node_search here?
do_refresh(Range, [Contact|T], IDs) ->
    ID = azdht:nodeid(Contact),
    Continue = case azdht_net:find_node(ID) of
        {error, timeout} ->
            true;
        {_, CloseNodes} ->
            do_refresh_inserts(Range, CloseNodes)
    end,
    case Continue of
        false -> ok;
        true  -> do_refresh(Range, T, [ID|IDs])
    end.

do_refresh_inserts({_, _}, []) ->
    true;
do_refresh_inserts({Min, Max}=Range, [Contact=#contact{node_id=ID}|T])
when ?in_range(ID, Min, Max) ->
    case safe_insert_node(Contact) of
        {error, timeout} ->
            do_refresh_inserts(Range, T);
        true ->
            do_refresh_inserts(Range, T);
        false ->
            safe_insert_nodes(T),
            false
    end;

do_refresh_inserts(Range, [Contact|T]) ->
    _ = safe_insert_node(Contact),
    do_refresh_inserts(Range, T).

spawn_refresh(Range, InputInactive, InputActive) ->
    spawn_link(fun() -> refresh(Range, InputInactive, InputActive) end).


max_unreachable() ->
    128.

unreachable_tab() ->
    azdht_unreachable_cache_tab.

random_node_tag() ->
    random:seed(now()),
    random:uniform(max_unreachable()).

%% @private
init([MyContact, StateFile]) ->
    % Initialize the table of unreachable nodes when the server is started.
    % The safe_ping and unsafe_ping functions aren't exported outside of
    % of this module so they should fail unless the server is not running.
    _ = case ets:info(unreachable_tab()) of
        undefined ->
            ets:new(unreachable_tab(), [named_table, public, bag]);
        _ -> ok
    end,

    MyNodeID = azdht:node_id(MyContact),
    NodeList = load_state(StateFile, MyNodeID),

    % Insert any nodes loaded from the persistent state later
    % when we are up and running. Use unsafe insertions or the
    % whole state will be lost if azdht starts without
    % internet connectivity.
    [spawn_link(fun() -> unsafe_insert_node(Contact) end)
    || Contact <- NodeList],

    #state{
        buckets=Buckets,
        buck_timers=InitBTimers,
        node_timeout=NTimeout,
        buck_timeout=BTimeout} = #state{},

    Now = os:timestamp(),
    BTimers = lists:foldl(fun(Range, Acc) ->
        BTimer = bucket_timer_from(Now, NTimeout, Now, BTimeout, Range),
        add_timer(Range, Now, BTimer, Acc)
    end, InitBTimers, b_ranges(Buckets)),

    State = #state{
        my_contact=MyContact,
        node_id=MyNodeID,
        buck_timers=BTimers,
        state_file=StateFile},
    {ok, State}.

%% @private
handle_call({is_interesting, Contact}, _From, State) ->
    #state{
        node_id=Self,
        buckets=Buckets,
        node_timeout=NTimeout,
        node_timers=NTimers} = State,
    IsInteresting = case b_is_member(Contact, Buckets) of
        true -> false;
        false ->
            ID = azdht:node_id(Contact),
            BMembers = b_members(ID, Buckets),
            Inactive = inactive_nodes(BMembers, NTimeout, NTimers),
            case (Inactive =/= []) or (length(BMembers) < ?K) of
                true -> true;
                false ->
                    TryBuckets = b_insert(Self, Contact, Buckets),
                    b_is_member(Contact, TryBuckets)
            end
    end,
    {reply, IsInteresting, State};

handle_call({insert_node, Contact}, _From, State) ->
    ID   = azdht:node_id(Contact),
    Now  = os:timestamp(),
    #state{
        node_id=Self,
        buckets=PrevBuckets,
        node_timers=PrevNTimers,
        buck_timers=PrevBTimers,
        node_timeout=NTimeout,
        buck_timeout=BTimeout} = State,

    IsPrevMember = b_is_member(Contact, PrevBuckets),
    Inactive = case IsPrevMember of
        true  -> [];
        false ->
            PrevBMembers = b_members(ID, PrevBuckets),
            inactive_nodes(PrevBMembers, NTimeout, PrevNTimers)
    end,

    {NewBuckets, Replace} = case {IsPrevMember, Inactive} of
        {true, _} ->
            % If the node is already a member of the node set,
            % don't change a thing
            {PrevBuckets, none};

        {false, []} ->
            % If there are no disconnected nodes in the bucket
            % insert it anyways and check later if it was actually added
            {b_insert(Self, Contact, PrevBuckets), none};

        {false, [OldContact|_]} ->
            % If there is one or more disconnected nodes in the bucket
            % Remove the old one and insert the new node.
            TmpBuckets = b_delete(OldContact, PrevBuckets),
            {b_insert(Self, Contact, TmpBuckets), OldContact}
    end,

    % If the new node replaced a new, remove all timer and access time
    % information from the state
    TmpNTimers = case Replace of
        none ->
            PrevNTimers;
        {_, _, _}=DNode ->
            del_timer(DNode, PrevNTimers)
    end,

    IsNewMember = b_is_member(Contact, NewBuckets),
    NewNTimers  = case {IsPrevMember, IsNewMember} of
        {false, false} ->
            TmpNTimers;
        {true, true} ->
            TmpNTimers;
        {false, true}  ->
            NTimer = node_timer_from(Now, NTimeout, Contact),
            add_timer(Contact, Now, NTimer, TmpNTimers)
    end,

    NewBTimers = case {IsPrevMember, IsNewMember} of
        {false, false} ->
            PrevBTimers;
        {true, true} ->
            PrevBTimers;

        {false, true} ->
            AllPrevRanges = b_ranges(PrevBuckets),
            AllNewRanges  = b_ranges(NewBuckets),

            DelRanges  = ordsets:subtract(AllPrevRanges, AllNewRanges),
            NewRanges  = ordsets:subtract(AllNewRanges, AllPrevRanges),

            DelBTimers = lists:foldl(fun(Range, Acc) ->
                del_timer(Range, Acc)
            end, PrevBTimers, DelRanges),

            lists:foldl(fun(Range, Acc) ->
                BMembers = b_members(Range, NewBuckets),
                LRecent = least_recent(BMembers, NewNTimers),
                BTimer = bucket_timer_from(
                             Now, BTimeout, LRecent, NTimeout, Range),
                add_timer(Range, Now, BTimer, Acc)
            end, DelBTimers, NewRanges)
    end,
    NewState = State#state{
        buckets=NewBuckets,
        node_timers=NewNTimers,
        buck_timers=NewBTimers},
    {reply, ((not IsPrevMember) and IsNewMember), NewState};




handle_call({closest_to, ID, NumNodes, WithMe}, _, State) ->
    #state{
        buckets=Buckets,
        node_timers=NTimers,
        node_timeout=NTimeout,
        my_contact=MyContact} = State,
    AllNodes   = b_node_list(Buckets),
    Active     = active_nodes(AllNodes, NTimeout, NTimers),
    Contacts   = [MyContact || WithMe] ++ Active,
    CloseNodes = azdht:closest_to(ID, Contacts, NumNodes),
    {reply, CloseNodes, State};


handle_call({request_timeout, Node}, _, State) ->
    Now  = os:timestamp(),
    #state{
        buckets=Buckets,
        node_timeout=NTimeout,
        node_timers=PrevNTimers} = State,

    NewNTimers = case b_is_member(Node, Buckets) of
        false ->
            State;
        true ->
            {LActive, _} = get_timer(Node, PrevNTimers),
            TmpNTimers   = del_timer(Node, PrevNTimers),
            NTimer       = node_timer_from(Now, NTimeout, Node),
            add_timer(Node, LActive, NTimer, TmpNTimers)
    end,
    NewState = State#state{node_timers=NewNTimers},
    {reply, ok, NewState};

handle_call({request_success, Node}, _, State) ->
    ID   = azdht:node_id(Node),
    Now  = os:timestamp(),
    #state{
        buckets=Buckets,
        node_timers=PrevNTimers,
        buck_timers=PrevBTimers,
        node_timeout=NTimeout,
        buck_timeout=BTimeout} = State,
    NewState = case b_is_member(Node, Buckets) of
        false ->
            State;
        true ->
            Range = b_range(ID, Buckets),

            {NLActive, _} = get_timer(Node, PrevNTimers),
            TmpNTimers    = del_timer(Node, PrevNTimers),
            NTimer        = node_timer_from(Now, NTimeout, Node),
            NewNTimers    = add_timer(Node, NLActive, NTimer, TmpNTimers),

            {BActive, _} = get_timer(Range, PrevBTimers),
            TmpBTimers   = del_timer(Range, PrevBTimers),
            BMembers     = b_members(Range, Buckets),
            LNRecent     = least_recent(BMembers, NewNTimers),
            BTimer       = bucket_timer_from(
                               BActive, BTimeout, LNRecent, NTimeout, Range),
            NewBTimers    = add_timer(Range, BActive, BTimer, TmpBTimers),

            State#state{
                node_timers=NewNTimers,
                buck_timers=NewBTimers}
    end,
    {reply, ok, NewState};


handle_call({request_from, Contact}, From, State) ->
    handle_call({request_success, Contact}, From, State);

handle_call(dump_state, _From, State) ->
    #state{
        node_id=Self,
        buckets=Buckets,
        state_file=StateFile} = State,
    catch dump_state(StateFile, Self, b_node_list(Buckets)),
    {reply, State, State};

handle_call({dump_state, StateFile}, _From, State) ->
    #state{
        node_id=Self,
        buckets=Buckets} = State,
    catch dump_state(StateFile, Self, b_node_list(Buckets)),
    {reply, ok, State};

handle_call(node_list, _From, State) ->
    #state{
        buckets=Buckets} = State,
    {reply, b_node_list(Buckets), State};

handle_call(node_id, _From, State) ->
    #state{node_id=Self} = State,
    {reply, Self, State}.

%% @private
handle_cast(_, State) ->
    {noreply, State}.

%% @private
handle_info({inactive_node, Node}, State) ->
    Now = os:timestamp(),
    #state{
        buckets=Buckets,
        node_timers=PrevNTimers,
        node_timeout=NTimeout} = State,

    IsMember = b_is_member(Node, Buckets),
    HasTimed = case IsMember of
        false -> false;
        true  -> has_timed_out(Node, NTimeout, PrevNTimers)
    end,

    NewState = case {IsMember, HasTimed} of
        {false, false} ->
            State;
        {true, false} ->
            State;
        {true, true} ->
            lager:debug("Node at ~p timed out", [azdht:compact_contact(Node)]),
            spawn_keepalive(Node),
            {LActive,_} = get_timer(Node, PrevNTimers),
            TmpNTimers  = del_timer(Node, PrevNTimers),
            NewTimer    = node_timer_from(Now, NTimeout, Node),
            NewNTimers  = add_timer(Node, LActive, NewTimer, TmpNTimers),
            State#state{node_timers=NewNTimers}
    end,
    {noreply, NewState};

handle_info({inactive_bucket, Range}, State) ->
    Now = os:timestamp(),
    #state{
        buckets=Buckets,
        node_timers=NTimers,
        buck_timers=PrevBTimers,
        node_timeout=NTimeout,
        buck_timeout=BTimeout} = State,

    BucketExists = b_has_bucket(Range, Buckets),
    HasTimed = case BucketExists of
        false -> false;
        true  -> has_timed_out(Range, BTimeout, PrevBTimers)
    end,

    NewState = case {BucketExists, HasTimed} of
        {false, false} ->
            State;
        {true, false} ->
            State;
        {true, true} ->
            lager:debug("Bucket timed out"),
            BMembers   = b_members(Range, Buckets),
            _ = spawn_refresh(Range,
                    inactive_nodes(BMembers, NTimeout, NTimers),
                    active_nodes(BMembers, NTimeout, NTimers)),
            TmpBTimers = del_timer(Range, PrevBTimers),
            LRecent    = least_recent(BMembers, NTimers),
            NewTimer   = bucket_timer_from(
                            Now, BTimeout, LRecent, NTimeout, Range),
            NewBTimers = add_timer(Range, Now, NewTimer, TmpBTimers),
            State#state{buck_timers=NewBTimers}
    end,
    {noreply, NewState}.

%% @private
terminate(_, State) ->
    #state{
        node_id=Self,
        buckets=Buckets,
        state_file=StateFile} = State,
    dump_state(StateFile, Self, b_node_list(Buckets)).

dump_state(undefined, _Self, _NodeList) ->
    ok;
dump_state(Filename, Self, NodeList) ->
    PersistentState = [{node_id, Self},
                       {node_set, azdht:compact_contacts(NodeList)}],
    file:write_file(Filename, term_to_binary(PersistentState)).

load_state(undefined, _NewNodeId) ->
    [];
load_state(Filename, NewNodeId) ->
    ErrorFmt = "Failed to load state from ~s (~w)",
    case file:read_file(Filename) of
        {ok, BinState} ->
            case (catch load_state_(BinState)) of
                {'EXIT', Reason}  ->
                    ErrorArgs = [Filename, Reason],
                    lager:error(ErrorFmt, ErrorArgs),
                    [];
                {NewNodeId, Nodes} ->
                    lager:info("Loaded state from ~s", [Filename]),
                    Nodes;
                {_OldNodeId, Nodes} ->
                    lager:info("State from ~s is old", [Filename]),
                    Nodes
            end;
        {error, Reason} ->
            ErrorArgs = [Filename, Reason],
            lager:error(ErrorFmt, ErrorArgs),
            []
    end.

load_state_(BinState) ->
    PersistentState = binary_to_term(BinState),
    {value, {_, Self}}  = lists:keysearch(node_id, 1, PersistentState),
    {value, {_, Nodes}} = lists:keysearch(node_set, 1, PersistentState),
    {Self, azdht:contacts(Nodes)}.


%% @private
code_change(_, State, _) ->
    {ok, State}.


%
% Create a new bucket list
%
b_new() ->
    MinID = <<0:160>>,
    MaxID = <<-1:160>>,
    [{MinID, MaxID, []}].

%
% Insert a new node into a bucket list
%
b_insert(Self, Contact, Buckets) ->
    b_insert_(Self, azdht:node_id(Contact), Contact, Buckets).


b_insert_(Self, ID, Contact, [{Min, Max, Members}|T])
when ?in_range(ID, Min, Max), ?in_range(Self, Min, Max) ->
    NumMembers = length(Members),
    if  NumMembers < ?K ->
        NewMembers = ordsets:add_element(Contact, Members),
        [{Min, Max, NewMembers}|T];

        NumMembers == ?K ->
        Half = b_between(Min, Max),
        if  Half =/= Max, Half =/= Min ->
            Lower = [N || N <- Members, ?in_range(azdht:node_id(N), Min, Half)],
            Upper = [N || N <- Members, ?in_range(azdht:node_id(N), Half, Max)],
            WithSplit = [{Min, Half, Lower}, {Half, Max, Upper}|T],
            b_insert_(Self, ID, Contact, WithSplit);
        true -> 
           [{Min, Max, Members}|T]
        end
    end;

b_insert_(_, ID, Contact, [{Min, Max, Members}|T])
when ?in_range(ID, Min, Max) ->
    NumMembers = length(Members),
    if  NumMembers < ?K ->
            NewMembers = ordsets:add_element(Contact, Members),
            [{Min, Max, NewMembers}|T];
        NumMembers == ?K ->
            [{Min, Max, Members}|T]
    end;

b_insert_(Self, ID, Contact, [H|T]) ->
    [H|b_insert_(Self, ID, Contact, T)].

%% Get all ranges present in a bucket list
b_ranges(Buckets) ->
    [{Min, Max} || {Min, Max, _} <- Buckets].

%% @doc Delete a node from a bucket list
b_delete(_Contact, []) ->
    [];
b_delete(Contact=#contact{node_id=ID}, [{Min, Max, Members}|T])
when ?in_range(ID, Min, Max) ->
    NewMembers = ordsets:del_element(Contact, Members),
    [{Min, Max, NewMembers}|T];
b_delete(Contact, [H|T]) ->
    [H|b_delete(Contact, T)].

%% @doc Return all members of the bucket that this node is a member of
b_members({Min, Max}, [{Min, Max, Members}|_]) ->
    Members;
b_members({Min, Max}, [_|T]) ->
    b_members({Min, Max}, T);

b_members(ID, [{Min, Max, Members}|_])
when ?in_range(ID, Min, Max) ->
    Members;
b_members(ID, [_|T]) ->
    b_members(ID, T).


%% @doc Check if a node is a member of a bucket list
b_is_member(_Contact, []) ->
    false;
b_is_member(Contact=#contact{node_id=ID}, [{Min, Max, Members}|_])
when ?in_range(ID, Min, Max) ->
    lists:member(Contact, Members);
b_is_member(Contact, [_|T]) ->
    b_is_member(Contact, T).

%% @doc Check if a bucket exists in a bucket list
b_has_bucket({_, _}, []) ->
    false;
b_has_bucket({Min, Max}, [{Min, Max, _}|_]) ->
    true;
b_has_bucket({Min, Max}, [{_, _, _}|T]) ->
    b_has_bucket({Min, Max}, T).

%% @doc Return a list of all members, combined, in all buckets.
b_node_list([]) ->
    [];
b_node_list([{_, _, Members}|T]) ->
    Members ++ b_node_list(T).

%% @doc Return the range of the bucket that a node falls within
b_range(ID, [{Min, Max, _}|_]) when ?in_range(ID, Min, Max) ->
    {Min, Max};
b_range(ID, [_|T]) ->
    b_range(ID, T).

b_between(<<Min:160>>, <<Max:160>>) ->
    Diff = Max - Min,
    Half = Max - (Diff div 2),
    <<Half:160>>.



inactive_nodes(Nodes, Timeout, Timers) ->
    [N || N <- Nodes, has_timed_out(N, Timeout, Timers)].

active_nodes(Nodes, Timeout, Timers) ->
    [N || N <- Nodes, not has_timed_out(N, Timeout, Timers)].

timer_tree() ->
    gb_trees:empty().

get_timer(Item, Timers) ->
    gb_trees:get(Item, Timers).

add_timer(Item, ATime, TRef, Timers) ->
    TState = {ATime, TRef},
    gb_trees:insert(Item, TState, Timers).

del_timer(Item, Timers) ->
    {_, TRef} = get_timer(Item, Timers),
    _ = erlang:cancel_timer(TRef),
    gb_trees:delete(Item, Timers).

node_timer_from(Time, Timeout, Contact) ->
    Msg = {inactive_node, Contact},
    timer_from(Time, Timeout, Msg).

bucket_timer_from(Time, BTimeout, LeastRecent, NTimeout, Range) ->
    % In the best case, the bucket should time out N seconds
    % after the first node in the bucket timed out. If that node
    % can't be replaced, a bucket refresh should be performed
    % at most every N seconds, based on when the bucket was last
    % marked as active, instead of _constantly_.
    Msg = {inactive_bucket, Range},
    if
        LeastRecent <  Time ->
            timer_from(Time, BTimeout, Msg);
        LeastRecent >= Time ->
            SumTimeout = NTimeout + NTimeout,
            timer_from(LeastRecent, SumTimeout, Msg)
    end.


timer_from(Time, Timeout, Msg) ->
    Interval = ms_between(Time, Timeout),
    erlang:send_after(Interval, self(), Msg).

ms_since(Time) ->
    timer:now_diff(Time, os:timestamp()) div 1000.

ms_between(Time, Timeout) ->
    MS = Timeout - ms_since(Time),
    if MS =< 0 -> Timeout;
       MS >= 0 -> MS
    end.

has_timed_out(Item, Timeout, Times) ->
    {LastActive, _} = get_timer(Item, Times),
    ms_since(LastActive) > Timeout.

least_recent([], _) ->
    os:timestamp();
least_recent(Items, Times) ->
    ATimes = [element(1, get_timer(I, Times)) || I <- Items],
    lists:min(ATimes).
