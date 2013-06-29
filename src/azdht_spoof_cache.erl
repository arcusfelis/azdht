-module(azdht_spoof_cache).
-behaviour(gen_server).

% Public interface
-export([start_link/0,
         spoof_id/1,
         store_spoof_id/2]).

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


table() ->
    azdht_spoof_cache_tab.

srv_name() ->
    azdht_spoof_cache_server.

%% @doc Request a spoof ID from Contact.
spoof_id(Contact) ->
    case extract_spoof_id(Contact) of
        {ok, SpoofId} ->
            {ok, SpoofId};
        {error, _Reason} ->
            case azdht_net:spoof_id(Contact) of
                {ok, SpoofId} ->
%                   store_spoof_id(Contact, SpoofId),
                    {ok, SpoofId};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

extract_spoof_id(Contact) ->
    try ets:lookup_element(table(), node_id(Contact), 2)
    of SpoofId -> {ok, SpoofId}
    catch error:badarg -> {error, not_found}
    end.

store_spoof_id(Contact, SpoofId) ->
    ets:insert(table(), {node_id(Contact), SpoofId, now_int()}).

-record(state, {}).

%
% Type definitions and function specifications
%

%
% Public interface
%
start_link() ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [], []).

%% ==================================================================

init([]) ->
    ets:new(table(), [set, public, named_table]),
    timer:send_interval(timer:seconds(60), clean_timeout),
    State = #state{},
    {ok, State}.

handle_call(Msg, _, State) ->
    {stop, Msg, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(clean_timeout, State) ->
    Expired = now_int() - expiry_int(),
    MS = ets:fun2ms(fun({_, _, Created}) -> Created < Expired end),
    ets:select_delete(table(), MS),
    {noreply, State}.

terminate(_, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% ==================================================================

now_int() ->
    {MegaSecs, Secs, _} = now(),
    (MegaSecs * 1000000) + Secs.

expiry_int() ->
    %% 15 minutes.
    15 * 60.

