%% @doc AZDHT utility functions.
-module(azdht).
-export([contact/1,
         contact/2,
         contact/3,
         contacts/1,
         compact_contact/1,
         compact_contacts/1,
         my_contact/2,
         node_id/1,
         node_id/3,
         higher_or_equal_version/2,
         lower_version/2,
         proto_version_num/1,
         action_name/1,
         action_reply_name/1,
         action_request_name/1,
         action_request_num/1,
         action_reply_num/1,
         diversification_type/1,
         random_key/0,
         encode_key/1,
         closest_to/3,
         is_id_in_closest_contacts/3,
         furthest_contact/1
        ]).

%% Spoof id
-export([generate_spoof_key/0,
         spoof_id/1,
         spoof_id/4]).


-include_lib("azdht/include/azdht.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Long.MAX_VALUE = 9223372036854775807 = 2^63-1
-define(MAX_LONG, (1 bsl 63 - 1)).

%% @pure
my_contact(ExternalIP, ListenPort) ->
    ProtoVer = proto_version_num(supported),
    contact(ProtoVer, ExternalIP, ListenPort).

%% @pure
-spec contact(proto_version(), address()) -> contact().
contact(ProtoVer, {IP, Port}) ->
    contact(ProtoVer, IP, Port).

%% @pure
-spec contact(proto_version(), ipaddr(), portnum()) ->
    contact().
contact(ProtoVer, IP, Port) ->
    #contact{version=ProtoVer,
             address={IP, Port},
             node_id=node_id(ProtoVer, IP, Port)}.

%% Create a contact from the compact form.
%% @pure
contact({ProtoVer, IP, Port}) ->
    contact(ProtoVer, IP, Port).

contacts(Constants) ->
    [contact(Constant) || Constant <- Constants].

%% Make contacts easy to print and read.
%% @pure
compact_contacts(Contacts) ->
    [compact_contact(Contact) || Contact <- Contacts].
compact_contact(#contact{version=ProtoVer, address={IP, Port}}) ->
    {ProtoVer, IP, Port}.

%% Node ID calculation
%% ===================

%% @pure
-spec node_id(contact()) -> node_id().
node_id(#contact{node_id=NodeId}) ->
    NodeId.

%% See DHTUDPUtils.getNodeID(InetSocketAddress, byte) in vuze.
%% @pure
-spec node_id(proto_version(), ipaddr(), portnum()) -> node_id().
node_id(ProtoVer, IP, Port) ->
    crypto:sha(node_id_key(ProtoVer, IP, Port)).

%% @pure
-spec node_id_key(proto_version(), ipaddr(), portnum()) -> iolist().
node_id_key(Version, IP, Port) ->
    case {version_to_key_type(Version), ip_version(IP)} of
        {uow, ipv4}          -> uow_key({IP, Port});
        %% stick with existing approach for IPv6 at the moment
        {uow, ipv6}          -> restrict_ports2_key({IP, Port});
        {restrict_ports2, _} -> restrict_ports2_key({IP, Port});
        {restrict_ports,  _} -> restrict_ports_key({IP, Port});
        {none,            _} -> simple_key({IP, Port})
    end.

%% @pure
version_to_key_type(Version) ->
    case higher_or_equal_version(Version, restrict_id3) of
        true -> uow;
        false ->
            case higher_or_equal_version(Version, restrict_id_ports2) of
                true -> restrict_ports2;
                false ->
                    case higher_or_equal_version(Version, restrict_id_ports) of
                        true -> restrict_ports;
                        false -> none
                    end
            end
        end.


%% restrictions suggested by UoW researchers as effective at reducing Sybil opportunity but 
%% not so restrictive as to impact DHT performance
%% @pure
uow_key({{A,B,C,D}, Port}) ->
    K0 = ?MAX_LONG,
    K1 = ?MAX_LONG,
    K2 = 2500,
    K3 = 50,
    K4 = 5,
    %% long    result = address.getPort() % K4;
    R0 = Port rem K4,
    %% result = ((((long)bytes[3] << 8 ) &0x000000ff00L ) | result ) % K3;
    %% result = ((((long)bytes[2] << 16 )&0x0000ff0000L ) | result ) % K2;
    %% result = ((((long)bytes[1] << 24 )&0x00ff000000L ) | result ); // % K1;
    %% result = ((((long)bytes[0] << 32 )&0xff00000000L ) | result ); // % K0;
    R1 = ((D bsl  8) bor R0) rem K3,
    R2 = ((C bsl 16) bor R1) rem K2,
    R3 = ((B bsl 24) bor R2) rem K1,
    R4 = ((A bsl 32) bor R3) rem K0,
    integer_to_list(R4).
    

%% @pure
restrict_ports2_key({IP, Port}) ->
    %% more draconian limit, analysis shows that of 500,000 node addresses only
    %% 0.01% had >= 8 ports active. ( 1% had 2 ports, 0.1% 3)
    %% Having > 1 node with the same ID doesn't actually cause too much grief
    %% 
    %% ia.getHostAddress() + ":" + ( address.getPort() % 8 );
    [print_ip(IP), $:, $0 + (Port rem 8)].

%% @pure
restrict_ports_key({IP, Port}) ->
    %% limit range to around 2000 (1999 is prime)
    %% ia.getHostAddress() + ":" + ( address.getPort() % 1999 );
    io_lib:format("~s:~B", [print_ip(IP), Port rem 1999]).

%% @pure
simple_key({IP, Port}) ->
    io_lib:format("~s:~B", [print_ip(IP), Port]).

%% @pure
ip_version(IP) ->
    case tuple_size(IP) of
        4 -> ipv4;
        8 -> ipv6
    end.

%% @pure
print_ip({A,B,C,D}) ->
    io_lib:format("~B.~B.~B.~B", [A,B,C,D]);
print_ip({_,_,_,_, _,_,_,_}=IP) ->
    %% 39 chars
    io_lib:format("~4.16.0B:~4.16.0B:~4.16.0B:~4.16.0B:"
                  "~4.16.0B:~4.16.0B:~4.16.0B:~4.16.0B", tuple_to_list(IP)).

%% @impure
-spec furthest_contact(contact()) -> contact() | undefined.
furthest_contact(Contact) ->
    ID = node_id(Contact),
    CFactor = ?K * 2,
    %% Get alive nodes.
    case azdht_router:closest_to(ID, CFactor) of
        [] -> undefined;
        Closest -> lists:last(Closest)
    end.


%% @pure
-spec closest_to(node_id(), list(contact()), non_neg_integer()) ->
    list(nodeinfo()).
closest_to(ID, Contacts, NumContacts) ->
    WithDist = [{compute_distance(node_id(Contact), ID), Contact}
               || Contact <- Contacts],
    Sorted = lists:keysort(1, WithDist),
    Limited = lists:sublist(Sorted, NumContacts),
    [Contact || {_, Contact} <- Limited].

%% see DHTUDPUtils.isIDInClosestContacts
is_id_in_closest_contacts(TestID, TargetID, NumToConsider) ->
    Closest = azdht_router:closest_to(TargetID, NumToConsider),
    ClosestNodeIDs = [node_id(Contact) || Contact <- Closest],
    case lists:member(TestID, ClosestNodeIDs) of
        false -> false;
        true -> is_id_in_closest_contacts_1(ClosestNodeIDs, TestID,
                                            TargetID, NumToConsider, 0)
    end.

is_id_in_closest_contacts_1([NodeID|ClosestNodeIDs],
                            TestID, TargetID,
                            NumToConsider, NumCloser)
    when NumCloser < NumToConsider ->
    Diff = compute_and_compare_distances(NodeID, TestID, TargetID),
    IsCloser = case Diff < 0 of true -> 1; false -> 0 end,
    is_id_in_closest_contacts_1(ClosestNodeIDs,
                                TestID, TargetID,
                                NumToConsider, NumCloser + IsCloser);
is_id_in_closest_contacts_1([], _, _, _, _) -> true;
%% Enough nodes are closer.
is_id_in_closest_contacts_1(_, _, _, _, _) -> false.
        


%% Spoof ID
%% ========

%% @impure
spoof_id(SenderContact) ->
    MyContact = azdht_net:my_contact(),
    case azdht:furthest_contact(MyContact) of
        undefined -> 0;
        FurthestContact ->
            SecretKey = azdht_db:secret_key(),
            spoof_id(SenderContact, MyContact, FurthestContact,
                     SecretKey)
    end.

%% For main network (not CVS).
%% see control/impl/DHTControlImpl.java:generateSpoofID(originator_contact)
%% @pure
spoof_id(#contact{}=Contact, MyContact, FurthestContact, SecretKey) ->
    FurthestID = node_id(FurthestContact),
    OriginatorID = node_id(Contact),
    MyID = node_id(MyContact),
    %% make sure the originator is in our group
    Diff = compute_and_compare_distances(FurthestID, OriginatorID, MyID),
    if Diff < 0 -> 0;
       true -> generate_spoof_id(Contact, SecretKey)
    end.

generate_spoof_id(#contact{address={IP,_Port}}, SecretKey) ->
    Text = case IP of
               {A,B,C,D}          -> <<A,B,C,D,4,4,4,4>>;
               {A,B,C,D, _,_,_,_} -> <<A:16,B:16,C:16,D:16>>
           end,
    <<SpoofId:32/big, _/binary>> = crypto:des_ecb_encrypt(SecretKey, Text),
    SpoofId.
 
 
-spec compute_and_compare_distances(ID1, ID2, Pivot) -> integer() when
    ID1 :: node_id(),
    ID2 :: node_id(),
    Pivot :: node_id().
compute_and_compare_distances(<<H1,T1/binary>>,
                              <<H2,T2/binary>>,
                              <<H3,T3/binary>>) ->
    D1 = H1 bxor H3,
    D2 = H2 bxor H3,
    case D1 - D2 of
        0 -> compute_and_compare_distances(T1, T2, T3);
        Diff -> Diff
    end;
compute_and_compare_distances(<<>>, <<>>, <<>>) -> 0.

compute_distance(<<ID1:160>>, <<ID2:160>>) ->
    <<(ID1 bxor ID2):160>>.


%% Constants
%% =========

higher_or_equal_version(VersionName1, VersionName2) ->
    proto_version_num(VersionName1) >=
    proto_version_num(VersionName2).

lower_version(VersionName1, VersionName2) ->
    proto_version_num(VersionName1) <
    proto_version_num(VersionName2).

proto_version_num(VersionNum) when is_integer(VersionNum) ->
    VersionNum;
proto_version_num(VersionName) ->
    case VersionName of
    div_and_cont         -> 6;
    anti_spoof           -> 7;
    anti_spoof2          -> 8;
    fix_originator       -> 9;
    networks             -> 9;
    vivaldi              -> 10;
    remove_dist_add_ver  -> 11;
    xfer_status          -> 12;
    size_estimate        -> 13;
    vendor_id            -> 14;
    block_keys           -> 14;
    generic_netpos       -> 15;
    vivaldi_findvalue    -> 16;
    anon_values          -> 17;
    cvs_fix_overload_v1  -> 18;
    cvs_fix_overload_v2  -> 19;
    more_stats           -> 20;
    cvs_fix_overload_v3  -> 21;
    more_node_status     -> 22;
    longer_life          -> 23;
    replication_control  -> 24;
    restrict_id_ports    -> 32;
    restrict_id_ports2   -> 33;
    restrict_id_ports2x  -> 34;
    restrict_id_ports2y  -> 35;
    restrict_id_ports2z  -> 36;
    restrict_id3         -> 50;
    minimum_acceptable   -> 16;
    supported            -> 50
    end.


action_request_num(ActionName) when is_atom(ActionName) ->
    case ActionName of
        ping       -> 1024;
        find_node  -> 1028;
        find_value -> 1030;
        _          -> undefined
    end.

action_reply_num(ActionName) when is_atom(ActionName) ->
    case ActionName of
        ping       -> 1025;
        find_node  -> 1029;
        find_value -> 1031;
        _          -> undefined
    end.

action_name(ActionNum) when is_integer(ActionNum) ->
    case ActionNum of
        1024 -> ping;
        1025 -> ping;
        1028 -> find_node;
        1029 -> find_node;
        1030 -> find_value;
        1031 -> find_value;
        1032 -> error;
        _    -> undefined
    end.

action_reply_name(ActionNum) when is_integer(ActionNum) ->
    case ActionNum of
        1025 -> ping;
        1029 -> find_node;
        1031 -> find_value;
        1032 -> error; %% Special care
        _    -> undefined
    end.

action_request_name(ActionNum) when is_integer(ActionNum) ->
    case ActionNum of
        1024 -> ping;
        1028 -> find_node;
        1030 -> find_value;
        _    -> undefined
    end.

diversification_type(1) -> none;
diversification_type(2) -> frequency;
diversification_type(3) -> size.

diversification_type_num(none)      -> 1;
diversification_type_num(frequency) -> 2;
diversification_type_num(size)      -> 3.

%% Crypto
%% ======

%% Pad with bytes all of the same value as the number of padding bytes
%% This is the method recommended in [PKCS5], [PKCS7], and [CMS].
%% http://www.di-mgt.com.au/cryptopad.html
pkcs5_padding(Bin) when is_binary(Bin) ->
    case byte_size(Bin) of
        0 -> <<8,8,8,8,8,8,8,8>>;
        1 -> <<Bin/binary,7,7,7,7,7,7,7>>;
        2 -> <<Bin/binary,6,6,6,6,6,6>>;
        3 -> <<Bin/binary,5,5,5,5,5>>;
        4 -> <<Bin/binary,4,4,4,4>>;
        5 -> <<Bin/binary,3,3,3>>;
        6 -> <<Bin/binary,2,2>>;
        7 -> <<Bin/binary,1>>;
        8 -> Bin
    end.

generate_spoof_key() ->
    crypto:strong_rand_bytes(8).

random_key() ->
    crypto:strong_rand_bytes(20).

encode_key(Key) ->
    crypto:sha(Key).


-ifdef(TEST).
%% DES INPUT BLOCK  = f  o  r  _  _  _  _  _
%% (IN HEX)           66 6F 72 05 05 05 05 05
%% KEY              = 01 23 45 67 89 AB CD EF
%% DES OUTPUT BLOCK = FD 29 85 C9 E8 DF 41 40
pkcs5_padding_test_() ->
    crypto:start(),
    [?_assertEqual(crypto:des_ecb_encrypt(<<16#0123456789ABCDEF:64>>,
                                          <<16#666F720505050505:64>>),
                   <<16#FD2985C9E8DF4140:64>>),
     ?_assertEqual(pkcs5_padding(<<16#666F72:24>>),
                   <<16#666F720505050505:64>>)].
-endif.
