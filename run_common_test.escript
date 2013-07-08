#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -sname azdht_ct debug verbose
-mode(compile).
-include_lib("common_test/include/ct_event.hrl").

main(_) ->
    %% Start networking or `not_alive'.
    net_kernel:start([azdht_ct, shortnames]),
    %% Do not want it in `src'.
    %% Compile it by hand before loading (not done by CT).
    {ok, forward_h} = compile:file("test/forward_h.erl",
                                   [{outdir, "ebin"}]),
    RelNames = ["ebin"|filelib:wildcard("deps/*/ebin")],
    AbsNames = [filename:absname(X) || X <- RelNames],
    code:add_paths(AbsNames),
    file:make_dir("logs"),
    CollectorPid = spawn_collector(),
    register(collector, CollectorPid),
    %% We cannot use Pids or Refs here, because CT will write them 
    %% into `variables-*' and will try to read them with `file:consult/1'.
    %% It will fail.
    EH = {forward_h, [collector, forward]},
    Opts = [{dir, "test"},
            {logdir, "logs"},
            {suite, [azdht_SUITE]},
            {config, ["azdht_test.cfg"]},
            {event_handler, [EH]}],
    Result = ct:run_test(Opts),
    Events = untag_acc(forward, get_acc(CollectorPid)),
    TCases = [X || X=#event{name = tc_done} <- Events],
    [print_test_case_summary(X) || X <- TCases],
    %% Sometimes `Result' is `not_alive'.
    %% Sometimes `Result' is `ok' (means nothing).
    io:format("Result ~p~n", [Result]),
    case Result of
        {error, _Reason} -> halt(1);
        _ ->
            %% Ok, Failed, UserSkipped, AutoSkipped are integers.
            #event{data = {Ok, Failed, {UserSkipped, AutoSkipped}}} =
                lists:keyfind(test_stats, #event.name, Events),
            if Failed > 0; AutoSkipped > 0 -> halt(1);
               true -> ok
            end
    end.

print_test_case_summary(#event{data = {Suite, Case, Result}}) ->
    case Result of
        {skipped, Reason} ->
            io:format("Case ~p skipped with reason ~p.~n", [Case, Reason]),
            ok;
        {failed,  Reason} ->
            io:format("Case ~p failed with reason ~p.~n", [Case, Reason]),
            ok;
        ok -> ok
    end.


spawn_collector() ->
    spawn(fun() -> collect_cycle([]) end).

untag_acc(Tag, Acc) ->
    [Msg || {Tag, Msg} <- Acc].

get_acc(CollectorPid) ->
    Ref = monitor(process, CollectorPid),
    CollectorPid ! {get_acc, self(), Ref},
    receive
        {Ref, Acc} -> Acc;
        {'DOWN', Ref, pricess, CollectorPid, Reason} ->
            error(Reason)
    end.

collect_cycle(Acc) ->
    receive
        {get_acc, To, Ref} ->
            To ! {Ref, Acc};
        Mess ->
            collect_cycle([Mess|Acc])
    end.

