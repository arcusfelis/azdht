%% @author Uvarov Michael <arcusfelis@gmail.com>
%% @doc TODO
%% @end
-module(azdht_net).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).


% Public interface
-export([start_link/2,
         node_port/0,
         ping/1,
         find_node/2,
         find_value/2,
         get_peers/2,
         announce/4]).

-import(etorrent_azdht, [
         higher_or_equal_version/2,
         lower_version/2,
         proto_version_num/1,
         action_name/1,
         action_request_num/1,
         diversification_type/1,
         diversification_type_num/1
        ]).


-define(LONG_MSB, (1 bsl 63)).

%% Totally "magic" number.
-define(MAX_TRANSACTION_ID, 16#FFFFFF).
-define(MAX_UINT, 16#FFFFFFFF).

-include_lib("azdht/include/azdht.hrl").

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    socket :: inet:socket(),
    sent   :: gb_tree(),
    tokens :: queue(),

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

socket_options() ->
    [list, inet, {active, true}, {mode, binary}].
%   ++ case etorrent_config:listen_ip() of all -> []; IP -> [{ip, IP}] end.


%
% Public interface
%
start_link(DHTPort, ExternalIP) ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [DHTPort, ExternalIP], []).


-spec node_port() -> portnum().
node_port() ->
    gen_server:call(srv_name(), get_node_port).


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
        Values  -> decode_reply_body(find_node, Values)
            % TODO
%           etorrent_dht_state:log_request_success(ID, IP, Port),
    end.


find_value(Contact, EncodedKey) ->
    case gen_server:call(srv_name(), {find_value, Contact, EncodedKey}) of
        timeout ->
            {error, timeout};
        Values  ->
            decode_reply_body(find_value, Values)
    end.

%
%
%
-spec get_peers(contact(), infohash()) ->
    {nodeid(), token(), list(peerinfo()), list(nodeinfo())}.
get_peers(Contact, InfoHash) ->
    Call = {get_peers, Contact, InfoHash},
    case gen_server:call(srv_name(), Call) of
        timeout ->
            {error, timeout};
        Values ->
            decode_reply_body(get_peers, Values)
    end.


%
%
%
-spec announce(contact(), infohash(), token(), portnum()) ->
    {'error', 'timeout'} | nodeid().
announce(Contact, InfoHash, Token, BTPort) ->
    Announce = {announce, Contact, InfoHash, Token, BTPort},
    case gen_server:call(srv_name(), Announce) of
        timeout -> {error, timeout};
        Values -> decode_reply_body(announce, Values)
    end.


%% ==================================================================

%% @private
forward_reply(SocketPid, Address, Reply) ->
    gen_server:cast(SocketPid, {forward_reply, Address, Reply}).

%% ==================================================================

init([DHTPort, ExternalIP]) ->
    {ok, Socket} = gen_udp:open(DHTPort, socket_options()),
    LocalContact = etorrent_azdht:contact(proto_version_num(supported),
                                          ExternalIP, DHTPort),
    State = #state{socket=Socket,
                   sent=gb_trees:empty(),
                   local_contact=LocalContact,
                   node_address={ExternalIP, DHTPort},
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

handle_call({get_peers, Contact, InfoHash}, From, State) ->
    Action = get_peers,
    Args = InfoHash,
    do_send_query(Action, Args, Contact, From, State);

handle_call({announce, Contact, InfoHash, Token, BTPort}, From, State) ->
    Action = announce,
    Args = {InfoHash, Token, BTPort},
    do_send_query(Action, Args, Contact, From, State);

handle_call(get_node_port, _From, State) ->
    #state{
        socket=Socket} = State,
    {ok, {_, Port}} = inet:sockname(Socket),
    {reply, Port, State}.

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

handle_cast(not_implemented, State) ->
    {noreply, State}.

handle_info({timeout, _, IP, Port, ID}, State) ->
    #state{sent=Sent} = State,

    NewState = case find_sent_query(IP, Port, ID, Sent) of
        error ->
            State;
        {ok, {Client, _Timeout, Action}} ->
            _ = gen_server:reply(Client, timeout),
            NewSent = clear_sent_query(IP, Port, ID, Sent),
            State#state{sent=NewSent}
    end,
    {noreply, NewState};

handle_info({udp, _Socket, IP, Port, Packet},
            #state{instance_id=MyInstanceId} = State) ->
    io:format(user, "Receiving a packet from ~p:~p~n", [IP, Port]),
    io:format(user, "~p~n", [Packet]),
    SocketPid = self(),
    NewState =
    case packet_type(Packet) of
        request ->
            spawn_link(fun() ->
%                       etorrent_dht_state:safe_insert_node(IP, Port),
                        ok
                end),
            spawn_link(fun() ->
                        handle_request_packet(Packet,
                                              MyInstanceId,
                                              {IP, Port},
                                              SocketPid),
                        ok
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

code_change(_, _, State) ->
    {ok, State}.

%% ==================================================================

handle_request_packet(Packet, MyInstanceId, Address, SocketPid) ->
    {RequestHeader, Body} = decode_request_header(Packet),
    #request_header{
        action=ActionNum,
        transaction_id=TranId,
        connection_id=ConnId,
        protocol_version=Version
    } = RequestHeader,
    io:format("Decoded header: ~ts~n", [pretty(RequestHeader)]),
    io:format("Body: ~p~n", [Body]),
    Action = action_name(ActionNum),
    {RequestBody, _} = decode_request_body(Action, Version, Body),
    io:format("Decoded body: ~ts~n", [pretty(RequestBody)]),
    Result = 
    case Action of
        ping ->
            NetworkCoordinates = [#position{type=none}],
            Args = #ping_reply{network_coordinates=NetworkCoordinates},
            {ok, Args};
        _ ->
            {error, unknown_action}
    end,
    case Result of
        {ok, ReplyArgs} ->
        PacketVersion = min(proto_version_num(supported), Version),
        ReplyHeader = #reply_header{
            action=ActionNum,
            connection_id=ConnId,
            transaction_id=TranId,
            protocol_version=PacketVersion,
            vendor_id=0,
            network_id=0,
            instance_id=MyInstanceId},
        Reply = [encode_reply_header(ReplyHeader)
                |encode_reply_body(Action, PacketVersion, ReplyArgs)],
        forward_reply(SocketPid, Address, Reply),
        ok;
    {error, Reason} ->
        lager:debug("Error ~p.", [Reason]),
        ok
    end.

handle_reply_packet(Packet, IP, Port, State=#state{sent=Sent}) ->
    try decode_reply_header(Packet) of
    {ReplyHeader, Body} ->
        #reply_header{action=ActionNum,
                      connection_id=ConnId,
                      protocol_version=Version} = ReplyHeader,
        io:format(user, "Reply ~ts~n", [pretty(ReplyHeader)]),
        case action_name(ActionNum) of
        undefined ->
            lager:debug("Unknown action ~p.", [ActionNum]),
            State;
        ReplyAction ->
            case find_sent_query(IP, Port, ConnId, Sent) of
            error ->
                lager:debug("Ignore unexpected packet from ~p:~p", [IP, Port]),
                State;
            {ok, {Client, Timeout, _RequestAction}} ->
                _ = cancel_timeout(Timeout),
                _ = gen_server:reply(Client, {ReplyAction, Version, Body}),
                NewSent = clear_sent_query(IP, Port, ConnId, Sent),
                State#state{sent=NewSent}
            end
        end
    catch error:Reason ->
            Trace = erlang:get_stacktrace(),
            lager:error("Decoding error ~p.~nTrace: ~s.",
                        [Reason, format_trace(Trace)]),
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
%           lager:info("Sent ~w to ~w:~w", [Action, IP, Port]),

            NewSent = store_sent_query(IP, Port, ConnId, From, TRef, Action, Sent),
            NewState = State#state{
                    sent=NewSent,
                    next_transaction_id=next_transaction_id(TranId)},
            {noreply, NewState};
        {error, einval} ->
%           lager:error("Error (einval) when sending ~w to ~w:~w",
%                       [Action, IP, Port]),
            {reply, timeout, State};
        {error, eagain} ->
%           lager:error("Error (eagain) when sending ~w to ~w:~w",
%                       [Action, IP, Port]),
            {reply, timeout, State}
    end.


unique_connection_id(IP, Port, Open) ->
    ConnId = new_connection_id(),
    IsLocal  = gb_trees:is_defined(tkey(IP, Port, ConnId), Open),
    if IsLocal -> unique_connection_id(IP, Port, Open);
       true    -> ConnId
    end.

store_sent_query(IP, Port, ID, Client, Timeout, Action, Open) ->
    K = tkey(IP, Port, ID),
    V = tval(Client, Timeout, Action),
    gb_trees:insert(K, V, Open).

find_sent_query(IP, Port, ID, Open) ->
    case gb_trees:lookup(tkey(IP, Port, ID), Open) of
       none -> error;
       {value, Value} -> {ok, Value}
    end.

clear_sent_query(IP, Port, ID, Open) ->
    gb_trees:delete(tkey(IP, Port, ID), Open).

tkey(IP, Port, ID) ->
   {IP, Port, ID}.

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
decode_connection_id(<<1:1, H:63/big-integer, T/binary>>) -> {H, T}.
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
    {etorrent_azdht:contact(ProtoVer, Address), T1}.


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
encode_value_group(Values, Version) ->
    ValueCount = length(Values),
    [encode_short(ValueCount),
     [encode_value(Rec, Version) || Rec <- Values]].

%% see DHTUDPUtils.deserialiseTransportValuesArray
encode_value_groups(ValueGroups, Version) ->
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
    decode_contact(Originator),
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
encode_request_body(find_value, Version, #find_value_request{
                id=ID,
                flags=Flags,
                max_values=MaxValues}) ->
    [encode_sized_binary(ID), encode_byte(Flags), encode_byte(MaxValues)];
encode_request_body(ping, Version, _) ->
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
    decode_boolean(HasValues),
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
    encode_sized_bytes(Divs).


decode_request_body(ping, Version, Bin) ->
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
            diversifications=Divs
            },
    {Reply, Bin1}.

decode_error(Version, Bin) ->
    {'TODO', Bin}.

decode_request_header(Bin) ->
    {ConnId,  Bin1} = decode_connection_id(Bin),
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
    ReplyBody = decode_reply_body(error, 50, EncodedBody),
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


