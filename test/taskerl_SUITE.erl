-module(taskerl_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").


all() -> [
          happy_case_async,
          happy_case_sync,
          overflow,
          worker_crash,
          taskerl_exit
         ].


suite() ->
    [{timetrap,{seconds, 30}}].


init_per_suite(Config) ->
    Config.


end_per_suite(_) -> ok.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    case flush() of
        [] -> ok;
        Messages -> ct:pal("Messages in queue are: ~p", [Messages])
    end,
    ok.


%% =============================================================================
%% Test cases
%% =============================================================================

happy_case_async(_Config) ->
    TaskerlPid = create_taskerl(),

    Self = self(),
    WaitingFun = fun() -> Self ! {ping, self()}, receive {pong, Self} -> ok end end,

    taskerl:run_async(TaskerlPid, WaitingFun),
    taskerl:run_async(TaskerlPid, WaitingFun),

    receive
        {ping, WorkerPid} ->
            receive
                {ping, _} -> ct:fail("Unserialized work")
            after 1000 -> ok
            end,

            WorkerPid ! {pong, Self}

    after 3000 -> ct:fail("Work not scheduled")
    end,

    receive
        {ping, SameWorkerPid} ->
            SameWorkerPid ! {pong, Self}

    after 3000 -> ct:fail("Work not scheduled")
    end,

    ok.


happy_case_sync(_Config) ->
    TaskerlPid = create_taskerl(),

    Self = self(),
    RetValue = "test",
    WaitingFun = fun() -> Self ! {ping, self()}, receive {pong, Self} -> RetValue end end,

    Waiting = [spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun)) end) || _ <- [1,2]],

    receive
        {ping, WorkerPid} ->
            receive
                {ping, _} -> ct:fail("Unserialized work")
            after 1000 -> ok
            end,

            WorkerPid ! {pong, Self}

    after 3000 -> ct:fail("Work not scheduled")
    end,

    StillWaiting = receive
                       {'DOWN', Ref, process, Pid, Reason} ->
                           Reason = RetValue,
                           lists:delete({Pid, Ref}, Waiting)
                   after 3000 -> ct:fail("Function not returned in time")
                   end,

    receive
        {ping, SameWorkerPid} ->
            SameWorkerPid ! {pong, Self}

    after 3000 -> ct:fail("Work not scheduled")
    end,

    [] = receive
             {'DOWN', Ref2, process, Pid2, Reason2} ->
                 Reason2 = RetValue,
                 lists:delete({Pid2, Ref2}, StillWaiting)
         after 3000 -> ct:fail("Function not returned in time")
         end,

    ok.


overflow(_Config) ->
    TaskerlPid = create_taskerl(),

    WaitingFun = fun() -> timer:sleep(infinity) end,

    [taskerl:run_async(TaskerlPid, WaitingFun) || _ <- lists:seq(1,1000)],

    {error, {taskerl, queue_full}} = taskerl:run(TaskerlPid, WaitingFun),
    {error, {taskerl, queue_full}} = taskerl:run(TaskerlPid, WaitingFun),
    ok = taskerl:run_async(TaskerlPid, WaitingFun),
    %% TODO Use meck to wait and verify that logger is called
    %% TODO Consume some and check that it allows to consume more

    {st, _WorkerPid, _PendingRequest, Queue, QueueLength, _, MaxSize} = sys:get_state(TaskerlPid),

    QueueLength = 1000,
    MaxSize = 1000,
    QueueLength = length(queue:to_list(Queue)),

    ok.


worker_crash(_Config) ->
    %%TODO
    ok.

taskerl_exit(_Config) ->
    %%TODO
    ok.


%% =============================================================================
%% Internal functions
%% =============================================================================

create_taskerl() ->
    {ok, TaskerlPid} = taskerl:start_link(),
    TaskerlPid.


flush() ->
    receive M -> [M | flush()] after 0 -> [] end.


print(Something) ->
    ct:print("~p~n", [Something]).

