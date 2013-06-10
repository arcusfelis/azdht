-module(azdht_app).
-behaviour(application).

%% API
-export([start/0, start/1, stop/0]).

%% Callbacks
-export([start/2, stop/1]).

-define(APP, azdht).

start() ->
    start([]).

start(Config) ->
    %% Delete duplicates, expand compacted config.
    Config1 = lists:ukeysort(1, proplists:unfold(Config)),
    [application:set_env(?APP, Key, Val)
     || {Key, Val} <- Config1],
    % Load app file.
    application:load(?APP),
    {ok, Deps} = application:get_key(?APP, applications),
    true = lists:all(fun ensure_started/1, Deps),
    application:start(?APP).

stop() ->
    application:stop(?APP).


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

ensure_started(App) ->
    case application:start(App) of
        ok ->
            true;
        {error, {already_started, App}} ->
            true;
        Else ->
            error_logger:error_msg("Couldn't start ~p: ~p", [App, Else]),
            Else
    end.

