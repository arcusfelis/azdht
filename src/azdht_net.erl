%% @author Uvarov Michael <arcusfelis@gmail.com>
%% @doc TODO
%% @end
-module(azdht_net).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).


% Public interface
-export([start_link/3,
         node_port/0,
         my_contact/0,
         ping/1,
         find_node/2,
         find_value/2,
         store/4,
         send_data/2,

         announce/4,
         get_peers/2,
         spoof_id/1]).

%% These functions use the router.
-export([find_node/1,
         find_value/1]).

-import(azdht, [
         higher_or_equal_version/2,
         lower_version/2,
         proto_version_num/1,
         action_reply_num/1,
         action_reply_name/1,
         action_request_num/1,
         action_request_name/1,
         diversification_type/1,
         diversification_type_num/1,
         data_packet_type/1,
         data_packet_type_num/1
        ]).


-define(LONG_MSB, (1 bsl 63)).

%% Totally "magic" number.
-define(MAX_TRANSACTION_ID, 16#FFFFFF).
-define(MAX_UINT, 16#FFFFFFFF).

-include("azdht.hrl").
%% DHTUDPPacketReply.DHT_HEADER_SIZE
-define(DHT_REPLY_HEADER_SIZE,
       (8 %% PRUDPPacketReply.PR_HEADER_SIZE
       +8 %% con id
       +1 %% ver
       +1 %% net 
       +4 %% instance
       +1 %% flags
       )).

-define(DHT_FIND_VALUE_HEADER_SIZE,
        (?DHT_REPLY_HEADER_SIZE + 1 + 1 + 2)).

-define(INETSOCKETADDRESS_IPV4_SIZE, 7).
    
-define(DHTTRANSPORTCONTACT_SIZE,
        (2 + ?INETSOCKETADDRESS_IPV4_SIZE)).
        
% DHTUDPUtils.DHTTRANSPORTVALUE_SIZE_WITHOUT_VALUE;
-define(DHTTRANSPORTVALUE_SIZE_WITHOUT_VALUE,
        (17 + ?DHTTRANSPORTCONTACT_SIZE)).

-define(DHT_FIND_VALUE_TV_HEADER_SIZE,
        ?DHTTRANSPORTVALUE_SIZE_WITHOUT_VALUE).

% DHTUDPPacketHelper.PACKET_MAX_BYTES
-define(PACKET_MAX_BYTES, 1400).

-define(MAX_SIZE_FIND_VALUE_REPLY,
        (?PACKET_MAX_BYTES - ?DHT_FIND_VALUE_HEADER_SIZE)).

%% DHTUDPPacketData.MAX_DATA_SIZE
-define(MAX_DATA_SIZE,
        (?PACKET_MAX_BYTES - ?DHT_REPLY_HEADER_SIZE - 1 - 21 - 21 - 14)).

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    socket :: inet:socket(),
    sent   :: gb_trees:tree(),
    tokens :: queue:queue(),

    node_address :: address(),
    local_contact :: contact(),
    next_transaction_id :: transaction_id(),
    instance_id :: instance_id()
}).

%
% Type definitions and function specifications
%


%
% Contacts and settings
%
srv_name() ->
   azdht_socket_server.

query_timeout() ->
    3000.

socket_options(ListenIP) ->
    [inet, {active, true}, {mode, binary}]
    ++ case ListenIP of all -> []; ListenIP -> [{ip, ListenIP}] end.

%
% Public interface
%
start_link(ListenIP, ListenPort, ExternalIP) ->
    Args = [ListenIP, ListenPort, ExternalIP],
    gen_server:start_link({local, srv_name()}, ?MODULE, Args, []).


-spec node_port() -> portnum().
node_port() ->
    gen_server:call(srv_name(), get_node_port).

-spec my_contact() -> contact().
my_contact() ->
    gen_server:call(srv_name(), my_contact).

%
%
%
-spec ping(contact()) -> term().
ping(Contact) ->
    case gen_server:call(srv_name(), {ping, Contact}) of
        timeout -> {error, timeout};
        Values -> decode_reply_body(ping, Values)
    end.

%
%
%
-spec find_node(contact(), nodeid()) ->
    {'error', 'timeout'} | {nodeid(), list(nodeinfo())}.
find_node(Contact, Target)  ->
    case gen_server:call(srv_name(), {find_node, Contact, Target}) of
        timeout -> {error, timeout};
        Values  ->
            case decode_reply_body(find_node, Values) of
                {error, Reason} -> {error, Reason};
                {ok, Reply=#find_node_reply{spoof_id=SpoofId}} ->
                    azdht_spoof_cache:store_spoof_id(Contact, SpoofId),
                    azdht_router:log_request_from(Contact),
                    {ok, Reply}
            end
    end.

send_data(Contact, DataReq) ->
    send_data(Contact, DataReq, 3).

send_data(_, _, 0) ->
    %% Run out of retries.
    {error, dropped};
send_data(Contact, DataReq, RetryCounter) ->
    case gen_server:call(srv_name(), {send_data, Contact, DataReq}, 60000) of
        timeout ->
            receive_multi_reply(),
            {error, timeout};
        Values  ->
            self() ! {multi_reply, Values},
            case concat_data_requests(DataReq, receive_multi_reply()) of
                {ok, [Down], []} -> {ok, Down};
                {ok, Downs, Drops} ->
                    case send_multi_data(Contact, Drops, RetryCounter-1) of
                        {ok, ReDowns} ->
                            {ok, Down, []} = concat_data_requests(DataReq, Downs ++ ReDowns),
                            {ok, Down};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    end.

send_multi_data(Contact, [H|T], RetryCounter) ->
    %% TODO: This code is sequential, can be parallel.
    case send_data(Contact, H, RetryCounter) of
        {ok, Down} ->
            case send_multi_data(Contact, T, RetryCounter) of
                {ok, Downs} ->
                    {ok, [Down|Downs]};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} -> {error, Reason}
    end.

%% @private
concat_data_requests(DataReq, [_|_]=Values) ->
    Values1 = lists:keysort(#data_request.start_position, Values),
    concat_data_requests_1(DataReq, Values1).

%% @private
concat_data_requests_1(DataReq, [H|T]) ->
    concat_data_requests_2(DataReq, T, H, [], []).

%% @private
concat_data_requests_2(
    Q,
    [ #data_request{start_position=HS, length=HL, data=HD, total_length=TL}|T],
    A=#data_request{start_position=AS, length=AL, data=AD, total_length=TL},
    Downloaded, Dropped)
        when AS + AL =:= HS, HS + HL =< TL ->
    A2 = A#data_request{
        length=AL+HL,
        data = <<AD/binary, HD/binary>>},
    concat_data_requests_2(Q, T, A2, Downloaded, Dropped);
concat_data_requests_2(
   Q,
   [H=#data_request{start_position=HS, length=HL, total_length=TL}|T],
    A=#data_request{start_position=AS, length=AL, total_length=TL},
    Downloaded, Dropped) when HS + HL =< TL ->
    DS = AS + AL,
    DL = HS - DS,
    %% Detect dropped packet
    D = Q#data_request{start_position=DS, length=DL, total_length=TL},
    concat_data_requests_2(Q, T, H, [A|Downloaded], [D|Dropped]);
concat_data_requests_2(
      #data_request{length=QL},
    [],
    A=#data_request{start_position=AS, length=AL, total_length=TL},
    Downloaded, Dropped)
    when AS + AL =:= TL, QL =:= 0 ->
    %% unknown length
    {ok, {[A|Downloaded], Dropped}};
concat_data_requests_2(
    A=#data_request{start_position=QS, length=QL},
    [],
    A=#data_request{start_position=AS, length=AL},
    Downloaded, Dropped)
    when AS + AL =:= QS + QL ->
    %% known length
    {ok, {[A|Downloaded], Dropped}};
concat_data_requests_2(Q, [],
    A=#data_request{start_position=AS, length=AL, total_length=TL},
    Downloaded, Dropped) ->
    DS = AS + AL,
    DL = TL - DS,
    %% Detect dropped last packet
    D = Q#data_request{start_position=DS, length=DL, total_length=TL},
    {ok, {[A|Downloaded], [D|Dropped]}}.

-ifdef(TEST).

concat_data_requests_test_() ->
    Q = #data_request{key=k, packet_type=q, transfer_key=tk,
                      length=0, total_length=0, data= <<>>},
    R = #data_request{key=k, packet_type=r, transfer_key=tk,
                      length=5, total_length=20, data= <<0:40>>},
    %% There are 4 packets of size 5, total size is 20 bytes.
    R1 = R#data_request{start_position=0},
    R2 = R#data_request{start_position=5},
    Q2 = Q#data_request{start_position=5, length=5, total_length=20},
    R3 = R#data_request{start_position=10},
    R4 = R#data_request{start_position=15},
    R34 = R#data_request{start_position=10, length=10, data= <<0:80>>},
    R1234 = R#data_request{start_position=0, length=20, data= <<0:160>>},
    [?_assertEqual({ok, {[R34, R1], [Q2]}}, concat_data_requests(Q, [R1,R3,R4]))
    ,?_assertEqual({ok, {[R1234], []}}, concat_data_requests(Q, [R1,R2,R3,R4]))].

-endif.

%% @doc Request a spoof ID.
spoof_id(Contact) ->
    case find_node(Contact, azdht:node_id(Contact)) of
        {ok, #find_node_reply{spoof_id=SpoofId}} -> {ok, SpoofId};
        {error, Reason} -> {error, Reason}
    end.


find_value(Contact, EncodedKey) ->
    case gen_server:call(srv_name(), {find_value, Contact, EncodedKey}) of
        timeout ->
            receive_multi_reply(),
            {error, timeout};
        Values  ->
            self() ! {multi_reply, Values},
            Replies = [decode_reply_body(find_value, Values1)
                       || Values1 <- receive_multi_reply()],
            case lists:any(fun is_error/1, Replies) of
                true  ->
                    {error, Replies};
                false ->
                    {ok, merge_find_value_bodies([X || {ok, X} <- Replies])}
            end
    end.

%% @private
receive_multi_reply() ->
    receive
        {multi_reply, Values} -> [Values|receive_multi_reply()]
    after 0 -> []
    end.

%% @private
is_error({error, _}) -> true;
is_error({ok, _})    -> false.


%% @private
merge_find_value_bodies([#find_value_reply{values=V1, has_continuation=true},
                      H2=#find_value_reply{values=V2}|T]) ->
    merge_find_value_bodies([H2#find_value_reply{values=V1 ++ V2}|T]);
merge_find_value_bodies([H=#find_value_reply{has_continuation=false}]) ->
    H.


%
%
%
-spec store(contact(), spoof_id(), list(key()), list(value_group())) ->
    {'error', 'timeout'} | {ok, term()}.
store(Contact, SpoofID, Keys, ValueGroups) ->
    Msg = {store, Contact, SpoofID, Keys, ValueGroups},
    case gen_server:call(srv_name(), Msg) of
        timeout -> {error, timeout};
        Values ->
            case decode_reply_body(store, Values) of
                {error, Reason} ->
                    lager:error("Store to ~p failed with ~p.",
                                [azdht:compact_contact(Contact), Reason]),
                    {error, Reason};
                {ok, Result} ->
                    lager:info("Store to ~p successed.",
                                [azdht:compact_contact(Contact)]),
                    {ok, Result}
            end
    end.

announce(Contact, SpoofID, EncodedKey, MyPortBT) ->
    {MegaSecs, Secs, MicroSecs} = now(),
    Secs2 = MegaSecs * 1000000 + Secs,
    MicroSecs2 = Secs2 * 1000000 + MicroSecs,
    Value = #transport_value{
        version = Secs2,
        created = MicroSecs2,
        value = list_to_binary(integer_to_list(MyPortBT)),
        flags = 0,
        life_hours = 0,
        replication_control = 0,
        originator = my_contact()
    },
    store(Contact, SpoofID, [EncodedKey], [[Value]]).


%% ==================================================================
-spec find_node(nodeid()) -> list(contact()).
find_node(NodeID) ->
    Contacts = azdht_router:closest_to(NodeID),
    azdht_find_node:find_node(NodeID, Contacts).


-spec find_value(key()) -> list(value()).
find_value(Key) ->
    EncodedKey = azdht:encode_key(Key),
    Contacts = azdht_router:closest_to(EncodedKey),
    azdht_find_value:find_value(Key, Contacts).

%% @doc `find_value' for ETorrent.
get_peers(Key, Contacts) ->
    Values = azdht_find_value:find_value(Key, Contacts),
    lists:usort([{IP, Port}
     || #transport_value{value=Value, originator=#contact{address={IP,_}}}
        <- Values,
        {ok, Port} <- [decode_port(Value)], Port > 1024]).

decode_port(Bin) ->
    case string:to_integer(binary_to_list(Bin)) of
        {error, Reason} -> {error, Reason};
        {Num, _Tail} -> {ok, Num}
    end.

%% ==================================================================

%% @private
forward_reply(SocketPid, Address, Reply) ->
    gen_server:cast(SocketPid, {forward_reply, Address, Reply}).

%% Send program generated reply.
%% Because data packets uses one action for both request and reply (action 1035),
%% we need this function to retransmit data to the gen_server's client.
%% @private
push_reply(Address, ConnId, Reply, HasContinuation) ->
    Msg = {push_reply, Address, ConnId, Reply, HasContinuation},
    %% Server can submit reply to the client, using ConnId.
    gen_server:cast(srv_name(), Msg).
    

%% ==================================================================

init([ListenIP, ListenPort, ExternalIP]) ->
    Socket = open_socket(ListenIP, ListenPort),
    LocalContact = azdht:contact(proto_version_num(supported),
                                          ExternalIP, ListenPort),
    State = #state{socket=Socket,
                   sent=gb_trees:empty(),
                   local_contact=LocalContact,
                   node_address={ExternalIP, ListenPort},
                   next_transaction_id=new_transaction_id(),
                   instance_id=new_instance_id()},
    {ok, State}.

handle_call({ping, Contact}, From, State) ->
    Action = ping,
    Args = undefined,
    do_send_query(Action, Args, Contact, From, State);

handle_call({find_node, Contact, Target}, From, State) ->
    Action = find_node,
    Args = #find_node_request{id=Target},
    do_send_query(Action, Args, Contact, From, State);

handle_call({find_value, Contact, EncodedKey}, From, State) ->
    Action = find_value,
    Args = #find_value_request{id=EncodedKey},
    do_send_query(Action, Args, Contact, From, State);

handle_call({store, Contact, SpoofID, Keys, ValueGroups}, From, State) ->
    Action = store,
    Args = #store_request{spoof_id=SpoofID,
                          keys=Keys,
                          value_groups=ValueGroups},
    do_send_query(Action, Args, Contact, From, State);
handle_call({send_data, Contact, Args}, From, State) ->
    Action = data,
    do_send_query(Action, Args, Contact, From, State);

handle_call(get_node_port, _From, State) ->
    #state{socket=Socket} = State,
    {ok, {_, Port}} = inet:sockname(Socket),
    {reply, Port, State};
handle_call(my_contact, _From, State) ->
    #state{local_contact=MyContact} = State,
    {reply, MyContact, State}.


handle_cast({forward_reply, {IP, Port}, EncodedReply}, State) ->
    #state{socket=Socket} = State,
    case gen_udp:send(Socket, IP, Port, EncodedReply) of
        ok ->
            {noreply, State};
        {error, einval} ->
            {noreply, State};
        {error, eagain} ->
            {noreply, State}
    end;

handle_cast({push_reply, {IP, Port}, ConnId, Reply, HasContinuation}, State) ->
    #state{sent=Sent} = State,
    State2 = 
    case find_sent_query(IP, Port, ConnId, Sent) of
    error ->
        lager:debug("Ignore unexpected packet from ~p:~p", [IP, Port]),
        State;
    {ok, {Client, Timeout, Action}} ->
        case HasContinuation of
            false ->
                _ = cancel_timeout(Timeout),
                _ = gen_server:reply(Client, Reply),
                NewSent = clear_sent_query(IP, Port, ConnId, Sent),
                State#state{sent=NewSent};
            true ->
                %% Update timeout
                _ = cancel_timeout(Timeout),
                Sent2 = clear_sent_query(IP, Port, ConnId, Sent),
                TRef = timeout_reference(IP, Port, ConnId),
                Sent3 = store_sent_query(IP, Port, ConnId, Client, TRef, Action, Sent2),
                %% Reply to the client
                {Pid, _Ref} = Client,
                Pid ! {multi_reply, Reply},
                State#state{sent=Sent3}
        end
    end,
    {noreply, State2};

handle_cast(not_implemented, State) ->
    {noreply, State}.


handle_info({timeout, _, IP, Port, ID}, State) ->
    #state{sent=Sent} = State,

    NewState = case find_sent_query(IP, Port, ID, Sent) of
        error ->
            State;
        {ok, {Client, _Timeout, _Action}} ->
            _ = gen_server:reply(Client, timeout),
            NewSent = clear_sent_query(IP, Port, ID, Sent),
            State#state{sent=NewSent}
    end,
    {noreply, NewState};

handle_info({udp, _Socket, IP, Port, Packet},
            #state{instance_id=MyInstanceId} = State) ->
    lager:debug("Receiving a packet from ~p:~p~n", [IP, Port]),
    SocketPid = self(),
    NewState =
    case packet_type(Packet) of
        request ->
            spawn_link(fun() ->
                    try
                        handle_request_packet(Packet,
                                              MyInstanceId,
                                              {IP, Port},
                                              SocketPid)
                     catch error:Reason:Stacktrace ->
                        lager:error("Cannot handle a packet because ~p. stacktrace=~1000p",
                                    [Reason, Stacktrace]),
                        lager:debug("Packet is ~p.", [Packet]),
                        ok
                     end
                end),
            State;
        reply ->
            handle_reply_packet(Packet, IP, Port, State)
    end,
    {noreply, NewState};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% ==================================================================
%
open_socket(ListenIP, ListenPort) ->
    case gen_udp:open(ListenPort, socket_options(ListenIP)) of
        {ok, Socket} ->
            Socket;
        Other ->
            error({open_socket_failed, #{ip => ListenIP, port => ListenPort, reason => Other}})
    end.

handle_request_packet(Packet, MyInstanceId, Address, SocketPid) ->
    {RequestHeader, Body} = decode_request_header(Packet),
    #request_header{
        action=RequestActionNum,
        transaction_id=TranId,
        connection_id=ConnId,
        protocol_version=Version
    } = RequestHeader,
%   lager:debug("Decoded header: ~ts~n", [pretty(RequestHeader)]),
%   lager:debug("Body: ~p~n", [Body]),
    Action = action_request_name(RequestActionNum),
    {RequestBody, _} = decode_request_body(Action, Version, Body),
%   lager:debug("Decoded body: ~ts~n", [pretty(RequestBody)]),
    SenderContact = azdht:contact(Version, Address),
    lager:info("Incoming request ~p from ~p.", [Action, Address]),
    Result = 
    case Action of
        ping ->
            NetworkCoordinates = [#position{type=none}],
            Args = #ping_reply{network_coordinates=NetworkCoordinates},
            {result, [Args], [], []};
        store ->
            #store_request{
                spoof_id=SpoofId,
                keys=Keys,
                value_groups=ValueGroups} = RequestBody,
            case azdht_db:store_request(SpoofId, SenderContact,
                                        Keys, ValueGroups) of
                {ok, Divs} ->
                    Args = #store_reply{diversifications=Divs},
                    {result, [Args], [], []};
                {error, _Reason} -> {error, _Reason}
            end;
        find_node ->
            NetworkCoordinates = [#position{type=none}],
            #find_node_request{
                id=NodeID} = RequestBody,
            Contacts = azdht_router:closest_to(NodeID),
            SpoofId = azdht:spoof_id(SenderContact), 
            Args = #find_node_reply{
                spoof_id=SpoofId,
                dht_size=0,
                network_coordinates=NetworkCoordinates,
                contacts=Contacts},
            {result, [Args], [], []};
        find_value ->
            handle_find_value_request_packet(RequestBody, Version);
        data ->
            handle_data_request_packet(RequestBody);
        _ ->
            lager:debug("Unknown action ~p.~n"
                         "Head: ~ts~n"
                         "Body: ~ts",
                        [RequestActionNum,
                         pretty(RequestHeader),
                         pretty(RequestBody)]),
            {error, unknown_action}
    end,
    case Result of
        {result, ReplyArgsList, ClientContReplies, ClientReplies} ->
        [push_reply(Address, ConnId, Reply, true)  %% HasContinuation = true
         || Reply <- ClientContReplies],
        [push_reply(Address, ConnId, Reply, false) %% HasContinuation = false
         || Reply <- ClientReplies],
        ReplyActionNum = action_reply_num(Action),
        PacketVersion = min(proto_version_num(supported), Version),
        ReplyHeader = #reply_header{
            action=ReplyActionNum,
            connection_id=ConnId,
            transaction_id=TranId,
            protocol_version=PacketVersion,
            vendor_id=0,
            network_id=0,
            instance_id=MyInstanceId},
        %% One header for all outgoing packets
        EncodedReplyHeader = encode_reply_header(ReplyHeader),
        %% For each reply packet...
        [begin
            EncodedReplyBody = encode_reply_body(Action,
                                                 PacketVersion,
                                                 ReplyArgs),
            ReplyPacket = [EncodedReplyHeader|EncodedReplyBody],
            forward_reply(SocketPid, Address, ReplyPacket)
         end || ReplyArgs <- ReplyArgsList],
        lager:info("Forwarded reply on ~p.", [Action]),
        azdht_router:safe_insert_node(SenderContact),
        ok;
    {error, Reason} ->
        lager:error("Ignore error ~p.", [Reason]),
        ok
    end.

handle_find_value_request_packet(RequestBody, Version) ->
    #find_value_request{
        id=EncodedKey,
        max_values=MaxValues} = RequestBody,
    case azdht_db:find_value(EncodedKey, MaxValues) of
        [] ->
            Contacts = azdht_router:closest_to(EncodedKey),
            NetworkCoordinates = [#position{type=none}],
            Args = #find_value_reply{
                    has_continuation = false,
                    has_values=false,
                    network_coordinates=NetworkCoordinates,
                    contacts=Contacts
                    },
            {result, [Args], [], []};
        Values ->
            ValuesPerPacket = split_values_into_packets(Values),
            ValuesPerPacket1 =
            case higher_or_equal_version(Version, div_and_cont) of
                true  -> ValuesPerPacket;
                false -> [hd(ValuesPerPacket)] %% send one packet
            end,
            ArgsList = find_value_replies(ValuesPerPacket1, none),
            {result, ArgsList, [], []}
    end.

handle_data_request_packet(X=#data_request{
                packet_type = read_reply,
                start_position=StartPosition,
                length=Length,
                total_length=TotalLength
                }) ->
    EndPosition = StartPosition + Length,
    if EndPosition =:= TotalLength -> {result, [], [], [X]}; %% last
       EndPosition  <  TotalLength -> {result, [], [X], []}  %% wait for more
    end.


handle_reply_packet(Packet, IP, Port, State=#state{sent=Sent}) ->
    try decode_reply_header(Packet) of
    {ReplyHeader, Body} ->
        %% TODO: check instance_id
        #reply_header{action=ActionNum,
                      connection_id=ConnId,
                      protocol_version=Version} = ReplyHeader,
%       lager:debug("Received reply header ~ts~n", [pretty(ReplyHeader)]),
        SenderContact = azdht:contact(Version, IP, Port),
        spawn_link(fun() ->
                    azdht_router:safe_insert_node(SenderContact),
                    ok
            end),
        case action_reply_name(ActionNum) of
        undefined ->
            lager:debug("Unknown action ~p.", [ActionNum]),
            State;
        ReplyAction ->
            case find_sent_query(IP, Port, ConnId, Sent) of
            error ->
                lager:debug("Ignore unexpected packet from ~p:~p", [IP, Port]),
                State;
            {ok, {Client, Timeout, _RequestAction}} ->
                case has_continuation(ReplyAction, Body) of
                    false ->
                        _ = cancel_timeout(Timeout),
                        _ = gen_server:reply(Client, {ReplyAction, Version, Body}),
                        NewSent = clear_sent_query(IP, Port, ConnId, Sent),
                        State#state{sent=NewSent};
                    true ->
                        {Pid, _Ref} = Client,
                        Pid ! {multi_reply, {ReplyAction, Version, Body}},
                        State
                end
            end
        end
    catch error:Reason ->
            Trace = erlang:get_stacktrace(),
            lager:error("Decoding error ~p.~nTrace: ~s.",
                        [Reason, format_trace(Trace)]),
            lager:debug("Data: ~p~n", [Packet]),
            State
    end.

do_send_query(Action, Args, #contact{version=Version,
                                     address={IP, Port}}, From, State) ->
    #state{sent=Sent,
           socket=Socket} = State,
    #state{sent=Sent,
           socket=Socket,
           node_address=NodeAddress,
           next_transaction_id=TranId,
           instance_id=InstanceId} = State,
    ConnId = unique_connection_id(IP, Port, Sent),
    ActionNum = action_request_num(Action),
    PacketVersion = min(proto_version_num(supported), Version),
    [erlang:error({bad_action, Action, ActionNum})
     || not is_integer(ActionNum)],
    RequestHeader = #request_header{
        action=ActionNum,
        connection_id=ConnId,
        transaction_id=TranId,
        instance_id=InstanceId,
        local_protocol_version=proto_version_num(supported),
        node_address=NodeAddress,
        protocol_version=PacketVersion,
        time=milliseconds_since_epoch()
    },
    Request = [encode_request_header(RequestHeader)
              |encode_request_body(Action, PacketVersion, Args)],

    case gen_udp:send(Socket, IP, Port, Request) of
        ok ->
            TRef = timeout_reference(IP, Port, ConnId),
            lager:info("Sent ~w to ~w:~w", [Action, IP, Port]),

            NewSent = store_sent_query(IP, Port, ConnId, From, TRef, Action, Sent),
            NewState = State#state{
                    sent=NewSent,
                    next_transaction_id=next_transaction_id(TranId)},
            {noreply, NewState};
        {error, einval} ->
            lager:error("Error (einval) when sending ~w to ~w:~w",
                        [Action, IP, Port]),
            {reply, timeout, State};
        {error, eagain} ->
            lager:error("Error (eagain) when sending ~w to ~w:~w",
                        [Action, IP, Port]),
            {reply, timeout, State}
    end.


unique_connection_id(IP, Port, Sent) ->
    ConnId = new_connection_id(),
    IsLocal  = gb_trees:is_defined(tkey(IP, Port, ConnId), Sent),
    if IsLocal -> unique_connection_id(IP, Port, Sent);
       true    -> ConnId
    end.

store_sent_query(IP, Port, ConnId, Client, Timeout, Action, Sent) ->
    K = tkey(IP, Port, ConnId),
    V = tval(Client, Timeout, Action),
    gb_trees:insert(K, V, Sent).

find_sent_query(IP, Port, ConnId, Sent) ->
    K = tkey(IP, Port, ConnId),
    case gb_trees:lookup(K, Sent) of
       none ->
            lager:debug("Unexpected transaction ~p.", [K]),
            error;
       {value, Value} ->
            {ok, Value}
    end.

clear_sent_query(IP, Port, ConnId, Sent) ->
    K = tkey(IP, Port, ConnId),
    gb_trees:delete(K, Sent).

tkey(IP, Port, ConnId) ->
   {IP, Port, ConnId}.

tval(Client, TimeoutRef, Action) ->
    {Client, TimeoutRef, Action}.

timeout_reference(IP, Port, ID) ->
    Msg = {timeout, self(), IP, Port, ID},
    erlang:send_after(query_timeout(), self(), Msg).

cancel_timeout(TimeoutRef) ->
    erlang:cancel_timer(TimeoutRef).


%% ==================================================================
%% Serialization

decode_byte(<<H, T/binary>>) -> {H, T}.
decode_short(<<H:16/big-integer, T/binary>>) -> {H, T}.
decode_int(<<H:32/big-integer, T/binary>>) -> {H, T}.
decode_long(<<H:64/big-integer, T/binary>>) -> {H, T}.
decode_none(Bin) -> {undefined, Bin}.
decode_float(<<H:32/big-float, T/binary>>) -> {H, T}.
%% transport/udp/impl/DHTUDPUtils.java:    deserialiseVivaldi
decode_network_coordinates(<<EntriesCount, Bin/binary>>) ->
    decode_network_coordinate_n(Bin, EntriesCount, []).

decode_network_coordinate_n(Bin, 0, Acc) ->
    {lists:reverse(Acc), Bin};
decode_network_coordinate_n(Bin, Left, Acc) ->
    {Pos, Bin1} = decode_network_position(Bin),
    decode_network_coordinate_n(Bin1, Left-1, [Pos|Acc]).

decode_network_position(<<Type, Size, Body:Size/binary, Bin/binary>>) ->
    {decode_network_position_1(Type, Body), Bin}.

decode_network_position_1(0, _) ->
    #position{type=none};
decode_network_position_1(1, Bin) ->
    {X, Bin1} = decode_float(Bin),
    {Y, Bin2} = decode_float(Bin1),
    {Z, Bin3} = decode_float(Bin2),
    {E, <<>>} = decode_float(Bin3),
    #position{type=vivaldi_v1, x=X, y=Y, z=Z, error=E};
decode_network_position_1(_, _) ->
    #position{type=unknown}.

decode_boolean(<<0, T/binary>>) -> {false, T};
decode_boolean(<<1, T/binary>>) -> {true, T}.

decode_sized_binary(<<Len, H:Len/binary, T/binary>>) ->
    {H, T}.

decode_sized_bytes(<<Len, H:Len/binary, T/binary>>) ->
    {binary_to_list(H), T}.

decode_sized_binary2(<<Len:16/big-integer, H:Len/binary, T/binary>>) ->
    {H, T}.

decode_diversification_type(<<Type, Bin/binary>>) ->
    {diversification_type(Type), Bin}.


%% First byte indicates length of the IP address (4 for IPv4, 16 for IPv6);
%% next comes the address in network byte order;
%% the last value is port number as short
decode_address(<<4, A, B, C, D, Port:16/big-integer, T/binary>>) ->
    {{{A,B,C,D}, Port}, T}.


%% First byte indicates contact type, which must be UDP (1);
%% second byte indicates the contact's protocol version;
%% the rest is an address.
decode_contact(<<1, ProtoVer, T/binary>>) ->
    {Address, T1} = decode_address(T),
    {azdht:contact(ProtoVer, Address), T1}.


decode_contacts(Bin) ->
    {ContactsCount, Bin1} = decode_short(Bin),
    decode_contacts_n(Bin1, ContactsCount, []).

decode_contacts_n(Bin, 0, Acc) ->
    {lists:reverse(Acc), Bin};
decode_contacts_n(Bin, Left, Acc) ->
    {Contact, Bin1} = decode_contact(Bin),
    decode_contacts_n(Bin1, Left-1, [Contact|Acc]).

%% see DHTTransportUDPImpl:sendStore
-spec decode_keys(Bin) -> Keys when
    Bin :: binary(),
    Keys :: list(binary()).
decode_keys(Bin) ->
    %% MAX_KEYS_PER_PACKET = 255;
    %% 1 byte DHTUDPPacket.PACKET_MAX_BYTES 
    {KeyCount, Bin1} = decode_byte(Bin),
    decode_keys_n(Bin1, KeyCount, []).

decode_keys_n(Bin, 0, Acc) ->
    {lists:reverse(Acc), Bin};
decode_keys_n(Bin, Left, Acc) ->
    {Key, Bin1} = decode_key(Bin),
    decode_keys_n(Bin1, Left-1, [Key|Acc]).

decode_key(Bin) ->
    decode_sized_binary(Bin).

%% transport/udp/impl/DHTUDPUtils.deserialiseTransportValues
decode_value_groups(Bin, Version) ->
    {ValueGroupCount, Bin1} = decode_byte(Bin),
    decode_value_groups_n(Bin1, Version, ValueGroupCount, []).

decode_value_groups_n(Bin, _Version, 0, Acc) ->
    {lists:reverse(Acc), Bin};
decode_value_groups_n(Bin, Version, Left, Acc) ->
    {Value, Bin1} = decode_value_group(Bin, Version),
    decode_value_groups_n(Bin1, Version, Left-1, [Value|Acc]).


decode_value_group(Bin, Version) ->
    {ValueCount, Bin1} = decode_short(Bin),
    decode_values_n(Bin1, Version, ValueCount, []).

decode_values_n(Bin, _Version, 0, Acc) ->
    {lists:reverse(Acc), Bin};
decode_values_n(Bin, Version, Left, Acc) ->
    {Value, Bin1} = decode_value(Bin, Version),
    decode_values_n(Bin1, Version, Left-1, [Value|Acc]).

decode_value(Bin, PacketVersion) ->
    {Version, Bin1} =
    case higher_or_equal_version(PacketVersion, remove_dist_add_ver) of
        true  -> decode_int(Bin);
        false -> decode_none(Bin)
    end,
    %% final long  created     = is.readLong() + skew;
    {Created, Bin2} = decode_long(Bin1),
    %% MAX_VALUE_SIZE = 512
    {Value, Bin3} = decode_sized_binary2(Bin2),
    {Originator, Bin4} = decode_contact(Bin3),
    {Flags, Bin5} = decode_byte(Bin4),
    {LifeHours, Bin6} =
    case higher_or_equal_version(PacketVersion, longer_life) of
        true  -> decode_byte(Bin5);
        false -> decode_none(Bin5)
    end,
    {RepControl, Bin7} =
    case higher_or_equal_version(PacketVersion, replication_control) of
        true  -> decode_byte(Bin6);
        false -> decode_none(Bin6)
    end,
    ValueRec = #transport_value{
        version = Version,
        created = Created,
        value = Value,
        originator = Originator,
        flags = Flags,
        life_hours = LifeHours,
        replication_control = RepControl},
    {ValueRec, Bin7}.


encode_byte(X)  when is_integer(X) -> <<X>>.
encode_short(X) when is_integer(X) -> <<X:16/big-integer>>.
encode_int(X)   when is_integer(X) -> <<X:32/big-integer>>.
encode_long(X)  when is_integer(X) -> <<X:64/big-integer>>.
encode_none() -> [].
encode_float(X) -> <<X:32/big-float>>.

encode_boolean(true)  -> <<1>>;
encode_boolean(false) -> <<0>>.

encode_network_coordinates(NetworkCoordinates) ->
    [encode_byte(length(NetworkCoordinates)),
     [encode_network_position(Pos) || Pos <- NetworkCoordinates]].

encode_network_position(#position{type=none}) ->
    <<0, 0>>;
encode_network_position(#position{type=vivaldi_v1,
                                  x=X, y=Y, z=Z, error=E}) ->
    [<<1, 16>>, [encode_float(Value) || Value <- [X,Y,Z,E]]].


%% First byte indicates length of the IP address (4 for IPv4, 16 for IPv6);
%% next comes the address in network byte order;
%% the last value is port number as short
encode_address({{A,B,C,D}, Port}) ->
    <<4, A, B, C, D, Port:16/big-integer>>.

encode_contacts(Contacts) -> 
    ContactsCount = length(Contacts),
    [encode_short(ContactsCount),
     [encode_contact(Rec) || Rec <- Contacts]].

%% First byte indicates contact type, which must be UDP (1);
%% second byte indicates the contact's protocol version;
%% the rest is an address.
encode_contact(#contact{version=ProtoVer, address=Address}) ->
    <<1, ProtoVer, (encode_address(Address))/binary>>.

encode_keys(Keys) -> 
    KeyCount = length(Keys),
    [encode_byte(KeyCount),
     [encode_key(Key) || Key <- Keys]].

encode_key(Key) ->
    encode_sized_binary(Key).

encode_sized_binary(ID) when is_binary(ID) ->
    [encode_byte(byte_size(ID)), ID].

encode_sized_binary2(ID) when is_binary(ID) ->
    [encode_short(byte_size(ID)), ID].

encode_sized_bytes(Bytes) ->
    encode_sized_binary(list_to_binary(Bytes)).

encode_diversification_type(Type) ->
    encode_byte(diversification_type_num(Type)).

encode_request_header(#request_header{
        connection_id=ConnId,
        action=Action,
        transaction_id=TranId,
        protocol_version=ProtoVer,
        vendor_id=VendorId,
        network_id=NetworkId,
        local_protocol_version=LocalProtoVer,
        node_address=NodeAddress,
        instance_id=InstanceId,
        time=Time}) ->
    [encode_long(ConnId),
     encode_int(Action),
     encode_int(TranId),
     encode_byte(ProtoVer),
     encode_byte(VendorId),
     encode_int(NetworkId),
     encode_byte(LocalProtoVer),
     encode_address(NodeAddress),
     encode_int(InstanceId),
     encode_long(Time)
    ].

encode_reply_header(#reply_header{
        action=Action,
        transaction_id=TranId,
        connection_id=ConnId,
        protocol_version=ProtoVer,
        vendor_id=VendorId,
        network_id=NetworkId,
        instance_id=InstanceId
    }) ->
    [encode_int(Action),
     encode_int(TranId),
     encode_long(ConnId),
     encode_byte(ProtoVer),
     encode_byte(VendorId),
     encode_int(NetworkId),
     encode_int(InstanceId)
    ].

%% see DHTUDPUtils.deserialiseTransportValues
encode_value_group(Values, Version) when is_list(Values) ->
    ValueCount = length(Values),
    [encode_short(ValueCount),
     [encode_value(Rec, Version) || Rec <- Values]].

%% see DHTUDPUtils.deserialiseTransportValuesArray
encode_value_groups(ValueGroups, Version) when is_list(ValueGroups) ->
    ValueGroupCount = length(ValueGroups),
    %% MAX_KEYS_PER_PACKET = 255
    [encode_byte(ValueGroupCount),
     [encode_value_group(Group, Version) || Group <- ValueGroups]].


%% see decode_value/2
encode_value(ValueRec=#transport_value{}, PacketVersion) ->
    #transport_value{
        version = Version,
        created = Created,
        value = Value,
        originator = Originator,
        flags = Flags,
        life_hours = LifeHours,
        replication_control = RepControl} = ValueRec,
    [
    case higher_or_equal_version(PacketVersion, remove_dist_add_ver) of
        true  -> encode_int(Version);
        false -> encode_none()
    end,
    encode_long(Created),
    encode_sized_binary2(Value),
    encode_contact(Originator),
    encode_byte(Flags),
    case higher_or_equal_version(PacketVersion, longer_life) of
        true  -> encode_byte(LifeHours);
        false -> encode_none()
    end,
    case higher_or_equal_version(PacketVersion, replication_control) of
        true  -> encode_byte(RepControl);
        false -> encode_none()
    end].


encode_request_body(find_node, Version, #find_node_request{
                id=ID,
                node_status=NodeStatus,
                dht_size=DhtSize
                }) ->
    [encode_sized_binary(ID),
     [[encode_int(NodeStatus), encode_int(DhtSize)]
     || higher_or_equal_version(Version, more_node_status)]
    ];
encode_request_body(find_value, _Version, #find_value_request{
                id=ID,
                flags=Flags,
                max_values=MaxValues}) ->
    [encode_sized_binary(ID), encode_byte(Flags), encode_byte(MaxValues)];
encode_request_body(ping, _Version, _) ->
    [];
encode_request_body(store, Version, #store_request{
                spoof_id=SpoofId,
                keys=Keys,
                value_groups=ValueGroups
                }) ->
    [
    case higher_or_equal_version(Version, anti_spoof) of
        true -> encode_int(SpoofId);
        false -> encode_none()
    end,
    encode_keys(Keys),
    encode_value_groups(ValueGroups, Version)
    ];
encode_request_body(data, _Version, #data_request{
        packet_type=PacketTypeName,
        key=Key,
        transfer_key=TransferKey,
        start_position=StartPosition,
        length=Length,
        total_length=TotalLength,
        data=Data}) ->
    [encode_byte(data_packet_type_num(PacketTypeName)),
     encode_sized_binary(TransferKey),
     encode_sized_binary(Key),
     encode_int(StartPosition),
     encode_int(Length),
     encode_int(TotalLength),
     encode_sized_binary2(Data)
    ].

encode_reply_body(ping, Version, #ping_reply{
                network_coordinates=NetworkCoordinates}) ->
    case higher_or_equal_version(Version, vivaldi) of
        true  -> encode_network_coordinates(NetworkCoordinates);
        false -> encode_none()
    end;
encode_reply_body(find_node, Version, #find_node_reply{
                spoof_id=SpoofId,
                node_type=NodeType,
                dht_size=DhtSize,
                network_coordinates=NetworkCoordinates,
                contacts=Contacts}) ->
    [
    case higher_or_equal_version(Version, anti_spoof) of
        true -> encode_int(SpoofId);
        false -> encode_none()
    end,
    case higher_or_equal_version(Version, xfer_status) of
        true -> encode_int(NodeType);
        false -> encode_none()
    end,
    case higher_or_equal_version(Version, size_estimate) of
        true -> encode_int(DhtSize);
        false -> encode_none()
    end,
    %% TODO: encode byte() here in v51.
    case higher_or_equal_version(Version, vivaldi) of
        true  -> encode_network_coordinates(NetworkCoordinates);
        false -> encode_none()
    end,
    encode_contacts(Contacts)
    ];
encode_reply_body(find_value, Version, #find_value_reply{
                has_continuation=HasContinuation,
                has_values=HasValues,
                diversification_type=DivType,
                values=Values,
                contacts=Contacts,
                network_coordinates=NetworkCoordinates
                }) ->
    [
    case higher_or_equal_version(Version, div_and_cont) of
        true -> encode_boolean(HasContinuation);
        flase -> encode_none()
    end,
    encode_boolean(HasValues),
    case HasValues of
        true ->
            %% Encode values.
            [
            case higher_or_equal_version(Version, div_and_cont) of
                 true -> encode_diversification_type(DivType);
                false -> encode_none()
            end,
            encode_value_group(Values, Version)
            ];
        false ->
            [
            encode_contacts(Contacts),
            case higher_or_equal_version(Version, vivaldi) of
                true -> encode_network_coordinates(NetworkCoordinates);
                false -> encode_none()
            end
            ]
    end
    ];
encode_reply_body(store, _Version, #store_reply{diversifications=Divs}) ->
    encode_sized_bytes([diversification_type_num(Div) || Div <- Divs]).


decode_request_body(ping, _Version, Bin) ->
    {ping, Bin};
decode_request_body(find_node, Version, Bin) ->
    {ID, Bin1} = decode_sized_binary(Bin),
    {NodeStatus, Bin2} =
    case higher_or_equal_version(Version, more_node_status) of
        true -> decode_int(Bin1);
        false -> decode_none(Bin1)
    end,
    {DhtSize, Bin3} =
    case higher_or_equal_version(Version, more_node_status) of
        true -> decode_int(Bin2);
        false -> decode_none(Bin2)
    end,
    Request = #find_node_request{
        id=ID,
        node_status=NodeStatus,
        dht_size=DhtSize
    },
    {Request, Bin3};
decode_request_body(find_value, _Version, Bin) ->
    {ID,        Bin1} = decode_sized_binary(Bin),
    {Flags,     Bin2} = decode_byte(Bin1),
    {MaxValues, Bin3} = decode_byte(Bin2),
    Request = #find_value_request{
        id=ID,
        flags=Flags,
        max_values=MaxValues
    },
    {Request, Bin3};
decode_request_body(store, Version, Bin) ->
    {SpoofId, Bin1} =
    case higher_or_equal_version(Version, anti_spoof) of
        true -> decode_int(Bin);
        false -> decode_none(Bin)
    end,
    {Keys, Bin2} = decode_keys(Bin1),
    {ValueGroups, Bin3} = decode_value_groups(Bin2, Version),
    Request = #store_request{
        spoof_id=SpoofId,
        keys=Keys,
        value_groups=ValueGroups
    },
    {Request, Bin3};
decode_request_body(data, _Version, Bin) ->
    {PacketType, Bin1} = decode_byte(Bin),
    {TransferKey, Bin2} = decode_sized_binary(Bin1),
    {Key, Bin3} = decode_sized_binary(Bin2),
    {StartPosition, Bin4} = decode_int(Bin3),
    {Length, Bin5} = decode_int(Bin4),
    {TotalLength, Bin6} = decode_int(Bin5),
    {Data, Bin7} = decode_sized_binary2(Bin6),
    Request = #data_request{
        packet_type=data_packet_type(PacketType),
        key=Key,
        transfer_key=TransferKey,
        start_position=StartPosition,
        length=Length,
        total_length=TotalLength,
        data=Data},
    {Request, Bin7};
decode_request_body(_Action, _Version, Bin) ->
    {unknown, Bin}.


decode_reply_body(_Action, {error, Version, Bin}) ->
    try decode_error(Version, Bin) of
        {Rec, _Bin} -> {error, Rec}
        catch error:Reason ->
            Trace = erlang:get_stacktrace(),
            lager:error("Decoding error ~p.~nTrace: ~s.",
                        [Reason, format_trace(Trace)]),
            {error, Reason}
    end;
decode_reply_body(Action, {Action, Version, Bin}) ->
    try decode_reply_body(Action, Version, Bin) of
        {Rec, _Bin} -> {ok, Rec}
        catch error:Reason ->
            Trace = erlang:get_stacktrace(),
            lager:error("Decoding error ~p.~nTrace: ~s.",
                        [Reason, format_trace(Trace)]),
            {error, Reason}
    end;
decode_reply_body(ExpectedAction, {ReceivedAction, _Version, _Bin}) ->
    {error, {action_mismatch, ExpectedAction, ReceivedAction}}.

decode_reply_body(find_node, Version, Bin) ->
    {SpoofId, Bin1} =
    case higher_or_equal_version(Version, anti_spoof) of
        true -> decode_int(Bin);
        false -> decode_none(Bin)
    end,
    {NodeType, Bin2} =
    case higher_or_equal_version(Version, xfer_status) of
        true -> decode_int(Bin1);
        false -> decode_none(Bin1)
    end,
    {DhtSize, Bin3} =
    case higher_or_equal_version(Version, size_estimate) of
        true -> decode_int(Bin2);
        false -> decode_none(Bin2)
    end,
    %% TODO: Decode byte() here in v51.
    {NetworkCoordinates, Bin4} =
    case higher_or_equal_version(Version, vivaldi) of
        true -> decode_network_coordinates(Bin3);
        false -> decode_none(Bin3)
    end,
    {Contacts, Bin5} = decode_contacts(Bin4),
    Reply = #find_node_reply{
        spoof_id=SpoofId,
        node_type=NodeType,
        dht_size=DhtSize,
        network_coordinates=NetworkCoordinates,
        contacts=Contacts
        },
    {Reply, Bin5};
decode_reply_body(find_value, Version, Bin) ->
    {HasContinuation, Bin1} =
    case higher_or_equal_version(Version, div_and_cont) of
        true -> decode_boolean(Bin);
        flase -> {false, Bin}
    end,
    {HasValues, Bin2} = decode_boolean(Bin1),
    case HasValues of
        true ->
            %% Decode values.
            {DivType, BinV1} =
            case higher_or_equal_version(Version, div_and_cont) of
                 true -> decode_diversification_type(Bin2);
                 false -> {none, Bin2}
            end,
            {Values, _} = decode_value_group(BinV1, Version),
            Reply = #find_value_reply{
                has_continuation=HasContinuation,
                has_values=HasValues,
                diversification_type=DivType,
                values=Values},
            {Reply, BinV1};
        false ->
            {Contacts, BinC1} = decode_contacts(Bin2),
            {NetworkCoordinates, BinC2} =
            case higher_or_equal_version(Version, vivaldi) of
                true -> decode_network_coordinates(BinC1);
                false -> decode_none(BinC1)
            end,
            Reply = #find_value_reply{
                has_continuation=HasContinuation,
                has_values=HasValues,
                contacts=Contacts,
                network_coordinates=NetworkCoordinates},
            {Reply, BinC2}
    end;
decode_reply_body(ping, Version, Bin) ->
    {NetworkCoordinates, Bin1} =
    case higher_or_equal_version(Version, vivaldi) of
        true -> decode_network_coordinates(Bin);
        false -> decode_none(Bin)
    end,
    Reply = #ping_reply{
            network_coordinates=NetworkCoordinates
            },
    {Reply, Bin1};
decode_reply_body(store, _Version, Bin) ->
    {Divs, Bin1} = decode_sized_bytes(Bin),
    Reply = #store_reply{
            diversifications=[diversification_type(X) || X <- Divs]
            },
    {Reply, Bin1}.

decode_error(_Version, Bin) ->
    {Type, Bin1} = decode_int(Bin),
    TypeName = azdht:error_type(Type),
    case TypeName of
        wrong_address ->
            {SenderAddress, Bin2} = decode_address(Bin1),
            Error = #azdht_error{
                type=wrong_address,
                sender_address=SenderAddress},
            {Error, Bin2};
        key_blocked ->
            {KeyBlockRequest, Bin2} = decode_sized_binary(Bin1),
            {Signature, Bin3} = decode_sized_binary2(Bin2),
            Error = #azdht_error{
                type=key_blocked,
                key_block_request=KeyBlockRequest,
                signature=Signature},
            {Error, Bin3}
    end.

decode_request_header(Bin) ->
    {ConnId,  Bin1} = decode_long(Bin),
    {Action,  Bin2} = decode_int(Bin1),
    {TranId,  Bin3} = decode_int(Bin2),
    {Version, Bin4} = decode_byte(Bin3),
    {VendorId, Bin5} =
    case higher_or_equal_version(Version, vendor_id) of
        true -> decode_byte(Bin4);
        false -> decode_none(Bin4)
    end,
    {NetworkId, Bin6} =
    case higher_or_equal_version(Version, networks) of
        true -> decode_int(Bin5);
        false -> decode_none(Bin5)
    end,
    {LocalProtoVer, Bin7} =
    case higher_or_equal_version(Version, fix_originator) of
        true -> decode_byte(Bin6);
        false -> decode_none(Bin6)
    end,
    {NodeAddress,    Bin8} = decode_address(Bin7),
    {InstanceId,     Bin9} = decode_int(Bin8),
    {Time,           BinA} = decode_long(Bin9),
    {LocalProtoVer1, BinB} =
    case lower_version(Version, fix_originator) of
        true -> decode_byte(BinA);
        false -> {LocalProtoVer, BinA}
    end,
    Header = #request_header{
        connection_id=ConnId,
        action=Action,
        transaction_id=TranId,
        protocol_version=Version,
        vendor_id=VendorId,
        network_id=NetworkId,
        local_protocol_version=LocalProtoVer1,
        node_address=NodeAddress,
        instance_id=InstanceId,
        time=Time},
    {Header, BinB}.

decode_reply_header(Bin) ->
    {Action,   Bin1} = decode_int(Bin),
    {TranId,   Bin2} = decode_int(Bin1),
    {ConnId,   Bin3} = decode_long(Bin2),
    {Version,  Bin4} = decode_byte(Bin3),
    {VendorId, Bin5} =
    case higher_or_equal_version(Version, vendor_id) of
        true -> decode_byte(Bin4);
        false -> decode_none(Bin4)
    end,
    {NetworkId, Bin6} =
    case higher_or_equal_version(Version, networks) of
        true -> decode_int(Bin5);
        false -> decode_none(Bin5)
    end,
    {InstanceId, Bin7} = decode_int(Bin6),
    Header = #reply_header{
        action=Action,
        transaction_id=TranId,
        connection_id=ConnId,
        protocol_version=Version,
        vendor_id=VendorId,
        network_id=NetworkId,
        instance_id=InstanceId
    },
    {Header, Bin7}.

new_connection_id() ->
    ?LONG_MSB bor crypto:rand_uniform(0, ?LONG_MSB).

new_instance_id() ->
    crypto:rand_uniform(0, ?MAX_UINT+1).

%% Init a transaction counter.
new_transaction_id() ->
    crypto:rand_uniform(0, ?MAX_TRANSACTION_ID).

next_transaction_id(TranId) -> TranId + 1.


packet_type(<<1:1, _/bitstring>>) -> request;
packet_type(<<0:1, _/bitstring>>) -> reply.



milliseconds_since_epoch() ->
    {MegaSeconds, Seconds, MicroSeconds} = os:timestamp(),
    MegaSeconds * 1000000000 + Seconds * 1000 + MicroSeconds div 1000.

-ifdef(TEST).

decode_request_header_test_() ->
    Decoded = #request_header{
        connection_id=17154702304824391947,
        action=1024,
        transaction_id=4192055156,
        protocol_version=26,
        vendor_id=0,
        network_id=0,
        local_protocol_version=26,
        node_address={{2,94,163,239},7000},
        instance_id=1993199759,
        time=1370016047962},
    Encoded = <<238,17,190,219,82,161,249,11, %% connection_id
                0,0,4,0, %% action
                249,221,175,116, %% transaction_id
                26,  0, %% protocol_version, vendor_id
                0,0,0,0,  26, %% network_id, local_protocol_version
                4, 2,94,163,239, 27,88, %% node_address
                118,205,208,143, %% instance_id
                0,0,1,62,251,81,227,90>>, %% time
    [?_assertEqual({Decoded, <<>>},
                   decode_request_header(Encoded))
    ,?_assertEqual(Encoded,
                   iolist_to_binary(encode_request_header(Decoded)))
    ].

decode_find_node_reply_v50_test() ->
    Encoded = 
<<0,0,4,5,0,23,91,158,133,10,54,16,79,15,21,209,26,0,0,0,0,0,163,63,81,111,0,0,
  0,0,0,0,0,0,0,15,89,82,
  %% Vivaldi (Count=1, Type=1, Size=16)
  1,1,16,66,172,48,247,193,222,165,78,66,138,147,38,64,117,5,250,
  0,20,1,50,4,136,169,240,183,187,38,1,50,4,178,126,109,151,50,87,1,
  51,4,180,194,225,103,44,246,1,51,4,212,187,99,32,240,105,1,51,4,71,203,192,
  83,79,9,1,50,4,176,32,156,176,213,111,1,50,4,74,100,191,149,26,225,1,50,4,74,
  115,1,238,207,167,1,50,4,178,185,63,108,136,81,1,50,4,159,146,164,62,196,162,
  1,51,4,78,12,77,131,192,1,1,51,4,66,68,151,141,145,67,1,51,4,82,224,238,96,
  127,55,1,50,4,178,140,190,197,97,51,1,50,4,82,140,224,208,195,32,1,50,4,217,
  118,81,11,236,127,1,51,4,2,121,14,147,108,54,1,50,4,109,162,3,144,157,145,1,
  51,4,74,128,154,253,100,76,1,50,4,2,92,237,112,44,19>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    decode_reply_body(find_node, 50, EncodedBody).

decode_ping_reply_v50_test() ->
    Encoded = 
<<0,0,4,8,0,236,211,236,221,101,78,233,38,92,44,150,26,0,0,0,0,0,142,62,187,89,
  0,0,0,1,4,2,93,75,156,26,10>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    decode_reply_body(ping, 50, EncodedBody).

decode_find_value_reply_v50_test() ->
    Encoded = 
<<0,0,4,7,0,10,59,231,236,78,46,77,80,232,184,143,50,0,0,0,0,0,202,9,186,151,0,
  0,0,20,1,51,4,176,205,120,124,102,46,1,50,4,86,183,19,22,237,207,1,51,4,109,
  11,140,246,234,96,1,51,4,24,72,68,22,215,46,1,50,4,178,47,116,49,197,24,1,51,
  4,109,65,167,187,253,198,1,51,4,78,226,84,17,172,217,1,51,4,123,194,241,201,
  59,6,1,51,4,90,210,133,94,158,139,1,51,4,89,141,28,134,227,11,1,50,4,95,28,
  215,202,81,50,1,50,4,108,16,231,161,203,207,1,51,4,112,209,137,10,174,101,1,
  51,4,101,162,163,22,69,26,1,50,4,72,9,31,224,26,225,1,51,4,81,57,81,147,121,
  152,1,50,4,89,235,246,223,231,230,1,51,4,80,230,5,103,73,111,1,51,4,123,243,
  133,26,100,215,1,50,4,178,185,42,250,248,88,1,1,16,65,227,179,146,66,151,181,
  220,66,129,151,217,62,99,194,145>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    ReplyBody = decode_reply_body(find_value, 50, EncodedBody),
    io:format(user, "ReplyBody ~p~n", [ReplyBody]),
    ok.

decode_find_node_reply_v51_test() ->
    Encoded = 
<<0,0,4,5,0,120,226,75,200,246,233,208,221,127,170,110,51,0,0,0,0,0,163,63,81,
  111,0,0,0,0,0,0,0,0,0,0,12,61,183,1,1,16,193,221,144,79,65,150,127,122,63,
  105,164,39,62,205,235,26,0,20,1,51,4,118,101,47,145,213,84,1,51,4,190,106,
  222,2,94,199,1,51,4,78,150,53,246,185,235,1,51,4,41,158,60,202,191,95,1,50,4,
  83,220,95,207,119,158,1,51,4,2,97,248,123,139,219,1,50,4,83,246,159,11,113,
  236,1,51,4,212,187,99,32,240,105,1,51,4,58,164,96,46,61,198,1,50,4,213,87,
  241,72,27,218,1,51,4,171,97,180,43,132,48,1,50,4,2,135,7,205,134,76,1,50,4,
  176,110,235,193,184,16,1,51,4,94,1,16,218,132,159,1,50,4,95,56,157,238,51,
  119,1,50,4,92,115,205,224,220,103,1,51,4,108,84,170,113,232,78,1,51,4,78,12,
  77,131,192,1,1,51,4,66,68,151,141,145,67,1,50,4,109,191,7,149,130,76>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    io:format(user, "ReplyHeader ~p~n", [ReplyHeader]),
    ReplyBody = decode_reply_body(find_node, 51, EncodedBody),
    io:format(user, "ReplyBody ~p~n", [ReplyBody]),
    ok.

decode_find_value_reply_with_values_v50_test() ->
    %% #reply_header{action = 1031,transaction_id = 3814337,
    %%        connection_id = 9752011279686643679,protocol_version = 50,
    %%        vendor_id = 0,network_id = 0,instance_id = 2951449045}
    Encoded =
<<0,0,4,7,0,175,240,41,185,148,181,7,211,155,229,131,50,0,0,0,0,0,158,59,199,
  160,
  %% Body start.
  %% has_continuation, has_values, diversification_type, value count
  0,1,1, 0,16,
  %% 1370646969 (int) and 1370704828459 (long). now() is {1370,719297,957191}.
  81,178,105,185, 0,0,1,63,36,95,216,43,
  0,7,50,54,50,54,49,59,67,
  1,51,4,88,207,176,62,102,149,
  %% flags, life_hours, rep_control
  2,0,255,
  81,154,42,253, 0,0,1,63, 36,88,116,8,0,
  5,50,49,55,49,48,
  1,51,4,209,89,161,167,84,206,
  2,0,255,
  81,175,61,85,0,0,1,63, 36,
  122,130,128,0,7,51,55,49,52,57,59,67,1,51,4,60,241,15,105,145,29,2,0,255,81,
  178,13,107,0,0,1,63,35,1,49,5,0,5,49,54,49,57,54,1,51,4,83,177,171,201,63,67,
  2,0,255,81,178,199,67,0,0,1,63,35,66,114,44,0,5,49,48,51,57,50,1,51,4,183,
  157,160,3,40,152,2,0,255,81,176,251,244,0,0,1,63,35,71,109,220,0,1,48,1,51,4,
  186,207,113,221,249,71,0,0,255,81,178,218,171,0,0,1,63,34,238,210,255,0,5,50,
  52,48,53,49,1,51,4,117,136,10,194,93,243,2,0,255,81,176,23,188,0,0,1,63,35,
  165,18,138,0,5,51,49,49,51,57,1,51,4,98,179,10,35,121,163,2,0,255,81,178,51,
  5,0,0,1,63,35,134,245,230,0,5,51,56,50,48,50,1,51,4,62,216,212,19,149,58,2,0,
  255,81,179,95,179,0,0,1,63,36,173,8,109,0,7,53,48,54,49,52,59,67,1,51,4,201,
  3,185,215,197,182,2,0,255,81,179,2,55,0,0,1,63,35,65,99,174,0,5,51,56,55,57,
  54,1,51,4,190,234,75,40,151,140,2,0,255,81,178,158,133,0,0,1,63,35,114,43,55,
  0,5,49,54,48,54,48,1,51,4,190,152,196,103,62,188,2,0,255,81,173,198,175,0,0,
  1,63,35,114,140,38,0,7,50,50,53,55,56,59,67,1,51,4,86,46,168,242,88,50,2,0,
  255,81,178,170,7,0,0,1,63,35,77,192,251,0,5,51,48,49,56,51,1,51,4,190,79,23,
  78,117,231,2,0,255,81,178,254,117,0,0,1,63,35,199,12,90,0,5,49,50,52,54,48,1,
  51,4,182,68,97,193,48,172,2,0,255,81,175,52,94,0,0,1,63,35,221,119,221,0,5,
  54,48,53,56,48,1,51,4,98,169,170,101,236,164,2,0,255>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    io:format(user, "ReplyHeader ~p~n", [ReplyHeader]),
    ReplyBody = decode_reply_body(find_value, 50, EncodedBody),
    io:format(user, "ReplyBody ~s~n", [pretty(ReplyBody)]),
    ok.


decode_error_reply_v50_test() ->
    Encoded = <<0,0,4,8,0,130,225,204,154,253,215,52,255,72,14,158,50,0,0,0,
                0,0,202,9,186,151,0,0,0,1,4,2,93,190,244,27,88>>,
    {ReplyHeader, EncodedBody} = decode_reply_header(Encoded),
    ReplyBody = decode_error(50, EncodedBody),
    io:format(user, "ReplyBody ~p~n", [ReplyBody]),
    ok.


decode_network_coordinates_test_() ->
    [?_assertEqual(decode_network_coordinates(<<0,0,0,1,4,2,93,75,156,26,10>>),
                   {[], <<0,0,1,4,2,93,75,156,26,10>>})
    ,?_assertEqual(decode_network_coordinates(<<1,1,16,66,172,48,247,193,222,
                                         165,78,66,138,147,38,64,117,5,250>>),
                   {[#position{type = vivaldi_v1,
                               x = 86.09563446044922,
                               y = -27.83071517944336,
                               z = 69.28739929199219,
                               error = 3.8284897804260254}], <<>>})
    ].

-endif.

find_value_replies([Values|ValuesPerPacket], DivType) ->
    HasContinuation = ValuesPerPacket =/= [],
    Reply = #find_value_reply{
        has_continuation = HasContinuation,
        has_values=true,
        diversification_type=DivType,
        values = Values
        },
    [Reply|find_value_replies(ValuesPerPacket, DivType)];
find_value_replies([], _) ->
    [].

encoded_value_size(#transport_value{value=Value}) ->
    byte_size(Value) + ?DHT_FIND_VALUE_TV_HEADER_SIZE.

split_values_into_packets(Values) ->
    split_values_into_packets_1(Values, ?PACKET_MAX_BYTES, []).

split_values_into_packets_1([Value|Values], Left, Acc) ->
    Left1 = encoded_value_size(Value),
    if Left < 0 ->
        [lists:reverse(Acc)|split_values_into_packets(Values)];
       true ->
        split_values_into_packets_1(Values, Left1, [Value|Acc])
    end;
split_values_into_packets_1([], _, []) ->
    [];
split_values_into_packets_1([], _, Acc) ->
    [lists:reverse(Acc)].

%% ======================================================================
%% Helpers for debugging.

pretty(Term) ->
    io_lib_pretty:print(Term, fun record_definition/2).

record_definition(Name, FieldCount) ->
%   io:format(user, "record_definition(~p, ~p)~n", [Name, FieldCount]),
%   io:format(user, "record_definition_list() = ~p~n", [record_definition_list()]),
    record_definition_1(Name, FieldCount+1, record_definition_list()).

record_definition_1(Name, Size, [{Name, Size, Fields}|_]) ->
    Fields;
record_definition_1(Name, Size, [{_, _, _}|T]) ->
    record_definition_1(Name, Size, T);
record_definition_1(_Name, _Size, []) ->
    no.


-define(REC_DEF(Name),
        {Name, record_info(size, Name), record_info(fields, Name)}).

record_definition_list() ->
    [?REC_DEF(find_node_reply)
    ,?REC_DEF(find_node_request)
    ,?REC_DEF(request_header)
    ,?REC_DEF(reply_header)
    ,?REC_DEF(data_request)
    ].

format_trace([{M,F,A,PL}|T]) ->
    Line = proplists:get_value(line, PL),
    Str = case Line of
        undefined -> io_lib:format("~p:~p~s~n", [M,F,format_arity(A)]);
        _         -> io_lib:format("~p#~p:~p~s~n", [M,Line,F,format_arity(A)])
    end,
    [Str|format_trace(T)];
format_trace([]) ->
    [].

format_arity(A) when is_integer(A) ->
    io_lib:format("/~p", [A]);
format_arity([]) ->
    "()";
format_arity([H|T]) ->
    [io_lib:format("(~p", [H]),
     [io_lib:format(",~p", [X]) || X <- T],
     ")"];
format_arity(_) ->
    io_lib:format("", []).


has_continuation(find_value, <<1, _/binary>>) ->
    true;
has_continuation(_, _) ->
    false.

