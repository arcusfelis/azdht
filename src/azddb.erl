%% Torrent downloader do not support packets with types
%% `write_request' and `write_reply'.
-module(azddb).
-include("azdht.hrl").
-export([request_data/2,
         download_torrent/1,
         download_torrent/2]).
    
%% Debian torrent (infohash is 96534331D2D75ACF14F8162770495BD5B05A17A9).
%% azddb:read(<<16#96534331D2D75ACF14F8162770495BD5B05A17A9:160>>).
%% Debian peers.
%% azdht_net:find_value(<<16#96534331D2D75ACF14F8162770495BD5B05A17A9:160>>).

download_torrent(BinIH) ->
    %% com/aelitis/azureus/plugins/magnet/MagnetPlugin.java
    Values = azdht_net:find_value(BinIH),
    %% See org/gudy/azureus2/pluginsimpl/local/ddb/DDBaseTTTorrent.java
    %% max values = 256
    Contacts = [Contact || #transport_value{originator=Contact} <- Values],
    lager:info("Downloading a torrent ~p from ~B contacts.",
               [BinIH, length(Contacts)]),
    download_torrent_from_oneof(Contacts, BinIH).
    
download_torrent_from_oneof([Contact|Contacts], BinIH) ->
    case download_torrent(Contact, BinIH) of
        {error, Reason} ->
            lager:error("Failed to download torrent ~p from ~p, reason is ~p.",
                        [BinIH, azdht:compact_contact(Contact), Reason]),
            download_torrent_from_oneof(Contacts, BinIH);
        {ok, BETorrent} ->
            {ok, {Contact, BETorrent, Contacts}}
    end;
download_torrent_from_oneof([], _BinIH) ->
    {error, no_respond}.

%% Get metadata for BinIH from Contact.
request_data(Contact, BinIH) ->
    Key = crypto:sha(BinIH),
    %% DDBaseHelpers.getKey(type.getClass()).getHash()
    TransferKey = transfer_key(torrent),
    DataReq = #data_request{
        packet_type = read_request,
        transfer_key = TransferKey,
        key = Key,
        start_position = 0,
        length = 0,
        total_length = 0,
        data = <<>>},
    azdht_net:send_data(Contact, DataReq).

download_torrent(Contact, BinIH) ->
    case request_data(Contact, BinIH) of
        {ok, #data_request{data=Data}} ->
            EncryptedData = case Data of
                <<1, 0, Data1/binary>> -> Data1; %% not encrypted
                <<1, 1, Data1/binary>> -> azdht:decrypt_torrent(BinIH, Data1)
            end,
            {ok, EncryptedData};
        {error, Reason} ->
            {error, Reason}
    end.

transfer_key(torrent) ->
    %% DDBaseHelpers.getKey(DDBaseTTTorrent.class);
    crypto:sha("org.gudy.azureus2.pluginsimpl.local.ddb.DDBaseTTTorrent").
