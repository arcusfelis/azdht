%% @author Uvarov Michael <freeakk@gmail.com>
%% @end
-module(azdht_find_value).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).
-import(azdht, [
        compact_contact/1,
        compact_contacts/1,
        node_id/1]).



% Public interface
-export([find_value/2]).

-include_lib("azdht/include/azdht.hrl").

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    key, encoded_key, called_contacts, waiting_contacts,
    collected_values, answered_contacts, answered_count,
    subscribers
}).

%
% Type definitions and function specifications
%


%
% Contacts and settings
%


%
% Public interface
%
%-spec find_value(Key, Contacts) -> Values.
find_value(Key, Contacts) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [Key, Contacts], []),
    gen_server:call(Pid, subscribe, 60000).

%% ==================================================================

init([Key, Contacts]) ->
    EncodedKey = azdht:encode_key(Key),
    [async_find_value(Contact, EncodedKey) || Contact <- Contacts],
    State = #state{key=Key,
                   encoded_key=EncodedKey,
                   called_contacts=sets:from_list(Contacts),
                   waiting_contacts=[],
                   collected_values=[],
                   answered_count=0,
                   answered_contacts=sets:new(),
                   subscribers=[]},
    schedule_next_step(),
    {ok, State}.

handle_call(subscribe, From, State=#state{subscribers=Subscribers}) ->
    {noreply, State#state{subscribers=[From|Subscribers]}}.

handle_cast({async_find_value_reply, Contact,
             #find_value_reply{has_values=false,
                               contacts=ReceivedContacts}},
             #state{called_contacts=Contacts,
                    waiting_contacts=WaitingContacts,
                    encoded_key=EncodedKey}=State) ->
    lager:debug("Received reply from ~p with contacts:~n~p",
                [compact_contact(Contact), compact_contacts(ReceivedContacts)]),
    Contacts1 = drop_farther_contacts(ReceivedContacts, Contact, EncodedKey),
    Contacts2 = drop_duplicates(Contacts1, Contacts),
    {noreply, State#state{waiting_contacts=Contacts2 ++ WaitingContacts}};
handle_cast({async_find_value_reply, Contact,
             #find_value_reply{has_values=true, values=Values}},
             #state{answered_count=AnsweredCount,
                    answered_contacts=Answered,
                    collected_values=CollectedValues}=State) ->
    lager:debug("Received reply from ~p with values:~n~p",
                [compact_contact(Contact), Values]),
    State2 = State#state{answered_count=AnsweredCount+1,
                         answered_contacts=sets:add_element(Contact, Answered),
                         collected_values=Values ++ CollectedValues},
    {noreply, State2};
handle_cast({async_find_value_error, Contact, Reason}, State) ->
    lager:debug("~p is unreachable. Reason ~p.",
                [compact_contact(Contact), Reason]),
    {noreply, State}.

handle_info(next_step,
            State=#state{key=Key,
                         waiting_contacts=[],
                         collected_values=[]}) ->
    lager:debug("Key ~p was not found.", [Key]),
    reply_to_subscribers(State),
    {stop, normal, State};
handle_info(next_step,
            State=#state{collected_values=Values,
                         answered_count=AnsweredCount}) when AnsweredCount > 3 ->
    lager:debug("Values ~p", [Values]),
    reply_to_subscribers(State),
    {stop, normal, State};
handle_info(next_step,
            State=#state{collected_values=Values,
                         waiting_contacts=[],
                         answered_count=AnsweredCount}) when AnsweredCount > 0 ->
    lager:debug("Not enough nodes were called. Values ~p", [Values]),
    reply_to_subscribers(State),
    {stop, normal, State};
handle_info(next_step,
            State=#state{encoded_key=EncodedKey,
                         called_contacts=Contacts,
                         waiting_contacts=WaitingContacts}) ->
    %% Run the next search iteration.
    BestContacts = best_contacts(WaitingContacts, EncodedKey),
    lager:debug("Best contacts:~n~p", [BestContacts]),
    [async_find_value(Contact, EncodedKey) || Contact <- BestContacts],
    State2 = State#state{waiting_contacts=[],
                         called_contacts=sets:union(sets:from_list(BestContacts),
                                                    Contacts)},
    schedule_next_step(),
    {noreply, State2}.

terminate(_, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.

%% ==================================================================


async_find_value(Contact, EncodedKey) ->
    Parent = self(),
    spawn_link(fun() ->
            case azdht_net:find_value(Contact, EncodedKey) of
                {ok, Reply} ->
                    gen_server:cast(Parent, {async_find_value_reply, Contact, Reply});
                {error, Reason} ->
                    gen_server:cast(Parent, {async_find_value_error, Contact, Reason})
            end
        end).

compute_distance(<<ID1:160>>, <<ID2:160>>) ->
    <<(ID1 bxor ID2):160>>.


%% Returns all contacts with distance lower than the distance beetween
%% `BaseContact' and `EncodedKey'.
drop_farther_contacts(Contacts, BaseContact, EncodedKey) ->
    BaseDistance = compute_distance(node_id(BaseContact), EncodedKey),
    [C || C <- Contacts, compute_distance(node_id(C), EncodedKey) < BaseDistance].

drop_duplicates(UnfilteredContacts, ContactSet) ->
    [C || C <- UnfilteredContacts, not sets:is_element(C, ContactSet)].

best_contacts(Contacts, EncodedKey) ->
    D2C1 = [{compute_distance(node_id(C), EncodedKey), C} || C <- Contacts],
    D2C2 = lists:usort(D2C1),
    Best = lists:sublist(D2C2, 32),
    [C || {_D,C} <- Best].



schedule_next_step() ->
    erlang:send_after(5000, self(), next_step),
    ok.



reply_to_subscribers(#state{collected_values=Values,
                            subscribers=Subscribers}) ->
    [gen_server:reply(Subscriber, Values) || Subscriber <- Subscribers].
