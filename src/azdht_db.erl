-module(azdht_db).
-behaviour(gen_server).

% Public interface
-export([start_link/0,
         find_value/1,
         store_request/4,
         secret_key/0]).

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-import(azdht, [
        node_id/1]).

-include_lib("stdlib/include/ms_transform.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("azdht/include/azdht.hrl").

table() ->
    azdht_db_tab.

srv_name() ->
    azdht_db_server.

max_total_size() ->
    160.

secret_key() ->
    gen_server:call(srv_name(), secret_key).

-spec find_value(EncodedKey) ->  Values when
    EncodedKey :: key(),
    Values :: value_group().
find_value(EncodedKey) ->
    ets:lookup_element(table(), EncodedKey, 2).

-spec insert_value(EncodedKey, Value) -> ok when
    EncodedKey :: key(),
    Value :: value().
insert_value(EncodedKey, Value) ->
    ets:insert(table(), {EncodedKey, Value}).

total_size() ->
    ets:info(table(), size).


store_request(SpoofId, SenderContact, Keys, ValueGroups) ->
    MyContact = azdht_net:my_contact(),
    FurthestContact = azdht:furthest_contact(MyContact),
    SpoofId1 = azdht:spoof_id(SenderContact, MyContact,
                              FurthestContact, secret_key()), 
    CFactor = ?K,
    MyNodeId = node_id(MyContact),
    IsCorrect = SpoofId =:= SpoofId1 andalso
    case is_cache_forwarding(SenderContact, ValueGroups) of
        true ->
            %% Is the sender known and closest?
            is_known_closest(SenderContact, MyNodeId, CFactor);
        false ->
            %% Originator is the sender.
            true
    end,
    case IsCorrect of
        true ->
            Divs =
            [case azdht:is_id_in_closest_contacts(MyNodeId,
                                                  K, CFactor) of
                true ->
                    store_request_1(K, Vs);
                false ->
                    none
             end
            || {K,Vs} <- lists:zip(Keys, ValueGroups)],
            {ok, Divs};
        false ->
            {error, bad_originator}
    end.

store_request_1(Key, NewValues) ->
    OldValues = ets:lookup_element(table(), Key, 2),
    lists:foldl(
        fun
        ({NV, undefined}, TotalSize) ->
            case TotalSize > max_total_size() of
                true  -> {frequency, TotalSize}; %% TODO: not sure
                false -> insert_value(Key, NV), {none, TotalSize+1}
            end;
            
        ({NV, OV}, TotalSize) ->
            ets:delete_object(table(), OV),
            insert_value(Key, NV),
            {none, TotalSize}
        end,
        total_size(),
        sort_and_left_join(#transport_value.originator,
                           NewValues, OldValues)).

%% All members from L1.
sort_and_left_join(N, L1, L2) ->
    SL1 = lists:keysort(N, L1),
    SL2 = lists:keysort(N, L2),
    ordered_left_join(N, SL1, SL2).

ordered_left_join(N, [H1|T1], [H2|T2]) ->
    E1 = element(N, H1),
    E2 = element(N, H2),
    if E1 =:= E2 -> [{H1,H2}|ordered_left_join(N, T1, T2)];
       E1  <  E2 -> [{H1,undefined}|ordered_left_join(N, T1, [H2|T2])];
       true      -> ordered_left_join(N, [H1|T1], T2)
    end.

is_cache_forwarding(SenderContact, ValueGroups) ->
    %% If originator != sender, than it is cache forwarding.
    SenderNodeId = node_id(SenderContact),
    F1 = fun(#transport_value{originator=OriginatorContact}) ->
            OriginatorNodeId = node_id(OriginatorContact),
            SenderNodeId =/= OriginatorNodeId
         end,
    F2 = fun(Group) -> lists:any(F1, Group) end,
    lists:any(F2, ValueGroups).



-record(state, {
    secret_key :: binary()
}).

%
% Type definitions and function specifications
%

%
% Public interface
%
%-spec find_node(NodeId, Contacts) -> Values.
start_link() ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [], []).

%% ==================================================================

init([]) ->
    ets:new(table(), [bag]),
    timer:send_interval(timer:seconds(60), clean_timeout),
    State = #state{
        secret_key=azdht:generate_spoof_key()
    },
    {ok, State}.

handle_call(secret_key, _, State=#state{secret_key=SecretKey}) ->
    {reply, SecretKey, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(clean_timeout, State) ->
    Expired = now_long() - expiry_long(),
    MS = ets:fun2ms(fun({_, #transport_value{created=Created}}) ->
                    Created < Expired end),
    ets:select_delete(table(), MS),
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.

%% ==================================================================

now_long() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    (((MegaSecs * 1000000) + Secs) *  1000000) + MicroSecs.

expiry_long() ->
    %% 15 minutes.
    15 * 60 * 1000000.


is_known_closest(SenderContact, MyNodeId, CFactor) ->
    lists:member(SenderContact,
                 azdht_router:closest_to(MyNodeId, CFactor)).
