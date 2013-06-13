%% @author Uvarov Michael <freeakk@gmail.com>
%% @end
-module(azdht_find_node).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).
-import(azdht, [
        compact_contact/1,
        compact_contacts/1,
        node_id/1,
        compute_distance/2]).



% Public interface
-export([find_node/2]).

-include_lib("azdht/include/azdht.hrl").

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    node_id, called_contacts, waiting_contacts,
    answered_contacts, answered_count,
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
%-spec find_node(NodeId, Contacts) -> Values.
find_node(NodeId, Contacts) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [NodeId, Contacts], []),
    gen_server:call(Pid, subscribe, 60000).

%% ==================================================================

init([NodeId, Contacts]) ->
    [async_find_node(Contact, NodeId) || Contact <- Contacts],
    State = #state{node_id=NodeId,
                   called_contacts=sets:from_list(Contacts),
                   waiting_contacts=[],
                   answered_count=0,
                   answered_contacts=sets:new(),
                   subscribers=[]},
    schedule_next_step(),
    {ok, State}.

handle_call(subscribe, From, State=#state{subscribers=Subscribers}) ->
    {noreply, State#state{subscribers=[From|Subscribers]}}.

handle_cast({async_find_node_reply, Contact,
             #find_node_reply{contacts=ReceivedContacts}},
             #state{called_contacts=Contacts,
                    waiting_contacts=WaitingContacts,
                    answered_contacts=AnsweredContacts,
                    node_id=NodeId}=State) ->
    lager:debug("Received reply from ~p with contacts:~n~p",
                [compact_contact(Contact), compact_contacts(ReceivedContacts)]),
    Contacts1 = drop_farther_contacts(ReceivedContacts, Contact, NodeId),
    Contacts2 = drop_duplicates(Contacts1, Contacts),
    {noreply, State#state{waiting_contacts=Contacts2 ++ WaitingContacts,
                          answered_contacts=sets:add_element(Contact, AnsweredContacts)}};
handle_cast({async_find_node_error, Contact, Reason}, State) ->
    lager:debug("~p is unreachable. Reason ~p.",
                [compact_contact(Contact), Reason]),
    {noreply, State}.

handle_info(next_step,
            State=#state{waiting_contacts=[]}) ->
    reply_to_subscribers(State),
    {stop, normal, State};
handle_info(next_step,
            State=#state{node_id=NodeId,
                         called_contacts=Contacts,
                         waiting_contacts=WaitingContacts}) ->
    %% Run the next search iteration.
    BestContacts = best_contacts(WaitingContacts, NodeId),
    lager:debug("Best contacts:~n~p", [BestContacts]),
    [async_find_node(Contact, NodeId) || Contact <- BestContacts],
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


async_find_node(Contact, NodeId) ->
    Parent = self(),
    spawn_link(fun() ->
            case azdht_net:find_node(Contact, NodeId) of
                {ok, Reply} ->
                    gen_server:cast(Parent, {async_find_node_reply, Contact, Reply});
                {error, Reason} ->
                    gen_server:cast(Parent, {async_find_node_error, Contact, Reason})
            end
        end).


%% Returns all contacts with distance lower than the distance beetween
%% `BaseContact' and `NodeId'.
drop_farther_contacts(Contacts, BaseContact, NodeId) ->
    BaseDistance = compute_distance(node_id(BaseContact), NodeId),
    [C || C <- Contacts, compute_distance(node_id(C), NodeId) < BaseDistance].

drop_duplicates(UnfilteredContacts, ContactSet) ->
    [C || C <- UnfilteredContacts, not sets:is_element(C, ContactSet)].

best_contacts(Contacts, NodeId) when is_list(Contacts) ->
    D2C1 = [{compute_distance(node_id(C), NodeId), C} || C <- Contacts],
    D2C2 = lists:usort(D2C1),
    Best = lists:sublist(D2C2, 32),
    [C || {_D,C} <- Best].



schedule_next_step() ->
    erlang:send_after(5000, self(), next_step),
    ok.



reply_to_subscribers(#state{node_id=NodeId,
                            answered_contacts=Contacts,
                            subscribers=Subscribers}) ->
    %% Return alive contacts.
    BestContacts = best_contacts(sets:to_list(Contacts), NodeId),
    [gen_server:reply(Subscriber, BestContacts) || Subscriber <- Subscribers].
