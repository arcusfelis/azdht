-module(azdht_app).
-behaviour(application).

%% API
-export([start/0, start/1, stop/0]).

%% Callbacks
-export([start/2, stop/1]).


start() ->
    start([]).

start(Config) ->
    %% Delete duplicates, expand compacted config.
    Config1 = lists:ukeysort(1, proplists:unfold(Config)),
    [application:set_env(azdht, Key, Val)
     || {Key, Val} <- Config1],
    {ok, _} = application:ensure_all_started(azdht),
    ok.

stop() ->
    application:stop(azdht).


%% @private
start(_Type, _Args) ->
    case azdht_sup:start_link() of
        {ok, Pid} ->
            {ok, Pid};
        {error, Err} ->
            {error, Err}
    end.

%% @private
stop(_State) ->
    ok.

