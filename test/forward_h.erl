-module(forward_h).
-behaviour(gen_event).

-export([init/1,
	 terminate/2,
	 handle_info/2,
	 handle_call/2,
	 handle_event/2,
     code_change/3]).

-record(state, {dest, tag}).

init([Dest, Tag]) ->
    {ok, #state{dest=Dest, tag=Tag}}.

terminate(remove_handler, _) ->
    ok;
terminate(stop, _) ->
    ok;
terminate(Error, S) ->
    error_logger:error_report([{module, ?MODULE},
			       {self, self()},
			       {error, Error},
			       {state, S}]).

handle_event(Event, S=#state{dest=Dest, tag=Tag}) ->
    Dest ! {Tag, Event},
    {ok, S}.

handle_info(_, S) ->
    {ok, S}.

handle_call(_, _) ->
    error(badarg).

code_change(_, _, State) ->
    {ok, State}.
