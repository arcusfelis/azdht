-define(K, 20).
-type nodeinfo() :: etorrent_types:nodeinfo().
-type peerinfo() :: etorrent_types:peerinfo().
-type infohash() :: etorrent_types:infohash().
-type token() :: etorrent_types:token().
-type ipaddr() :: etorrent_types:ipaddr().
-type nodeid() :: etorrent_types:nodeid().
-type portnum() :: etorrent_types:portnum().

-type long()  :: non_neg_integer().
-type int()   :: non_neg_integer().
-type short() :: non_neg_integer().
%% -type byte() :: 0 .. 255.
-type address() :: {ipaddr(), portnum()}.
-type network_coordinates() :: term().
-type transaction_id() :: non_neg_integer().
-type instance_id() :: non_neg_integer().
-type position_version() :: atom().
-type node_id() :: <<_:160>>.
-type proto_version() :: atom() | non_neg_integer().
-type key() :: binary().
-type diversification() :: int().

-record(position, {
    x = 0 :: float(),
    y = 0 :: float(),
    z = 0 :: float(),
    error = 0 :: float(),
    type :: position_version()
}).

-record(contact, {
    version :: proto_version(),
    address :: address(),
    node_id :: node_id()
}).

-type contact() :: #contact{}.
-type contacts() :: [contact()].

-type position() :: #position{}.
-type diversification_type() :: none | frequency | size.

-record(request_header, {
    %% Random number with most significant bit set to 1.
    connection_id :: long(),
    %% Type of the packet.
    action :: int(),
    %% Unique number used through the communication;
    %% it is randomly generated at the start of the application and
    %% increased by 1 with each sent packet.
    transaction_id :: int(),
    %% version of protocol used in this packet.
    protocol_version :: byte(),
    %% ID of the DHT implementator; 0 = Azureus, 1 = ShareNet, 255 = unknown
    %% ≥VENDOR_ID
    vendor_id = 0 :: byte(),
    %% ID of the network; 0 = stable version; 1 = CVS version
    %% ≥NETWORKS
    network_id = 0 :: int(),
    %% Maximum protocol version this node supports;
    %% if this packet's protocol version is <FIX_ORIGINATOR
    %% then the value is stored at the end of the packet
    %% ≥FIX_ORIGINATOR
    local_protocol_version :: byte(),
    %% Address of the local node
    node_address :: address(),
    %% Application's helper number; randomly generated at the start
    instance_id :: int(),     
    %% Time of the local node;
    %% stored as number of milliseconds since Epoch.
    time :: long()
}).

-record(reply_header, {
    %% Type of the packet.
    action :: int(),
    %% Must be equal to TRANSACTION_ID from the request.
    transaction_id :: int(),     
    %% must be equal to CONNECTION_ID from the request.
    connection_id :: long(),
    %% Version of protocol used in this packet.
    protocol_version :: byte(),
    %% Same meaning as in the request.
    %% ≥VENDOR_ID
    vendor_id = 0 :: byte(),
    %% Same meaning as in the request.
    %% ≥NETWORKS
    network_id = 0 :: int(),
    %% Instance id of the node that replies to the request.
    instance_id :: int()
}).

-record(find_node_request, {
    %% ID to search
    id :: binary(),
    %% Node status.
    %% ≥MORE_NODE_STATUS
    node_status = 0 :: int(), 
    %% Estimated size of the DHT; Unknown value can be indicated as zero.
    %% ≥MORE_NODE_STATUS
    dht_size = 0 :: int()
}).

-record(find_node_reply, {
    %% Spoof ID of the requesting node;
    %% it should be constructed from information known about
    %% requesting contact and not easily guessed by others.
    %% ≥ANTI_SPOOF
    spoof_id :: int(),
    %% Type of the replying node;
    %% Possible values are
    %% 0 for bootstrap node,
    %% 1 for ordinary node and ffffffffh for unknown type.
    %% ≥XFER_STATUS
    node_type :: int(),
    %% Estimated size of the DHT;
    %% Unknown value can be indicated as zero.
    %% ≥SIZE_ESTIMATE
    dht_size :: int(),
    %% Network coordinates of replying node.
    %% ≥VIVALDI
    network_coordinates :: network_coordinates(),
    %% Number of carried contacts.
    contacts_count :: short(),
    %% List with contacts. 
    contacts :: contacts()
}).

-record(ping_reply, {
    network_coordinates :: [position()]
}).

-record(find_value_request, {
    %% ID (encoded key) to search.
    %% Key for which the values are requested.
    id :: binary(),
    %% Flags for the operation.
    flags = 0 :: byte(), 
    %% Maximum number of returned values. 
    max_values = 16 :: byte()
}).

-record(find_value_reply, {
    %% Indicates whether there is at least one other packet with values.
    %% protocol version ≥DIV_AND_CONT
    has_continuation :: boolean(), 
    %% Indicates whether this packet carries values or contacts.
    has_values  :: boolean(),
    %% Number of stored contacts.
    %% has_values == false
    contacts_count :: short(),
    %% Stored contacts that are close to the searched key.
    %% has_values == false
    contacts :: contacts(),
    %% Network coordinates of the replying node.
    %% HAS_VALUES == false && protocol version ≥VIVALDI_FINDVALUE
    network_coordinates ::  network_coordinates(),
    %% Type of key's diversification.
    %% HAS_VALUES == true && protocol version ≥DIV_AND_CONT
    diversification_type :: diversification_type(),
    %% Values that match searched key.
    %% HAS_VALUES == true
    values % value_group() 
}).

-record(store_request, {
    %% Spoof ID of the target node;
    %% it must be the same number as previously retrived
    %% through FIND_NODE reply.
    %% ≥ANTI_SPOOF
    spoof_id :: int(),
    %% Keys that the target node should store.
    keys :: [key()],
    %% Groups of values, one for each key;
    %% values are stored in the same order as keys.
    value_groups % [value_group()]
}).

-record(store_reply, {
    diversifications :: [diversification()]
}).

-record(transport_value, {
    version :: int(),
    created :: long(),
    %% <<"26261;C">> or <<"21710">>.
    value :: binary(),
    originator :: contact(),
    flags :: byte(),
    life_hours :: byte() | undefined,
    replication_control :: byte() | undefined
}).

