% Part of taskerl Erlang App
% MIT License
% Copyright (c) 2019 Jose Maria Perez Ramos

-module(taskerl_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").


all() -> [
          happy_case_async,
          happy_case_sync,
          happy_case_sync_only_ack,
          overflow,
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
    TaskerlPid = create_taskerl_sync_with_response(),

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
    TaskerlPid = create_taskerl_sync_with_response(),

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


happy_case_sync_only_ack(_Config) ->
    TaskerlPid = create_taskerl_sync_with_ack(),

    Self = self(),
    [ {taskerl, {ack, N}} = taskerl:run(
                              TaskerlPid,
                              fun() -> Self ! N, receive N -> ok end end
                             )
      || N <- lists:seq(1,5) ],

    ongoing   = taskerl:get_request_status(TaskerlPid, 1),
    queued    = taskerl:get_request_status(TaskerlPid, 2),
    undefined = taskerl:get_request_status(TaskerlPid, 6),

    WorkerPid = taskerl:get_worker(TaskerlPid),

    WorkerPid ! 2, %% Nothing happens, as it's still waiting for 1

    receive 2 -> ct:fail("Unexpected work")
    after 1000 -> ok
    end,

    [ receive
          N ->
              if N < 3 -> WorkerPid ! N;
                 true -> ok
              end
      after 1000 -> ct:fail("Work not scheduled: ~p", [N])
      end
      || N <- [1,2,3] ],

    finished = taskerl:get_request_status(TaskerlPid, 1),
    finished = taskerl:get_request_status(TaskerlPid, 2),
    ongoing  = taskerl:get_request_status(TaskerlPid, 3),
    queued   = taskerl:get_request_status(TaskerlPid, 4),

    ok.


overflow(_Config) ->
    QueueLimit = 50,
    TaskerlPid = create_taskerl_sync_with_response(QueueLimit),

    Self = self(),
    WaitingFun = fun(Expected) -> Self ! Expected, receive Expected -> ok end end,
    [ taskerl:run_async(TaskerlPid, WaitingFun, [N]) || N <- lists:seq(1,QueueLimit) ],

    {taskerl, {error, queue_full}} = taskerl:run(TaskerlPid, WaitingFun, [unexpected_work]),
    {taskerl, {error, queue_full}} = taskerl:run(TaskerlPid, WaitingFun, [unexpected_work]),
    ok = taskerl:run_async(TaskerlPid, WaitingFun, [unexpected_work]),

    receive unexpected_work -> ct:fail("Unexpected work")
    after 1000 -> ok
    end,

    queued    = taskerl:get_request_status(TaskerlPid, QueueLimit),
    undefined = taskerl:get_request_status(TaskerlPid, QueueLimit + 1),

    WorkerPid = taskerl:get_worker(TaskerlPid),

    [ receive
          N -> WorkerPid ! N
      after 1000 -> ct:fail("Work not scheduled: ~p", [N])
      end
      || N <- lists:seq(1,QueueLimit) ],

    receive unexpected_work -> ct:fail("Unexpected work")
    after 1000 -> ok
    end,

    WorkerPid = taskerl:run(TaskerlPid, fun() -> self() end),

    ok.


taskerl_exit(_Config) ->
    TaskerlPid = create_taskerl_sync_with_response(),

    Self = self(),
    WaitingFun = fun(Expected) -> Self ! Expected, receive Expected -> Expected end end,

    Waitings = [ spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [N])) end) || N <- lists:seq(1,5) ],

    receive 1 -> ok
    after 1000 -> ct:fail("Work not scheduled")
    end,

    process_flag(trap_exit, true),
    exit(TaskerlPid, shutdown),

    Returns = [ receive
                    {'DOWN', Ref, process, _, RetValue} -> RetValue
                after 1000 -> ct:fail("Missing return value")
                end
                || {_Pid, Ref} <- Waitings ],

    NotScheduled = [{taskerl, {error, not_scheduled}} || _ <- lists:seq(1,4)],
    [ {shutdown, _} | NotScheduled ] = Returns,

    ok.


%% =============================================================================
%% Internal functions
%% =============================================================================

create_taskerl_sync_with_response() ->
    create_taskerl_sync_with_response(1000).

create_taskerl_sync_with_response(QueueLimit) ->
    {ok, TaskerlPid} = taskerl:start_link(false, QueueLimit),
    TaskerlPid.

create_taskerl_sync_with_ack() ->
    {ok, TaskerlPid} = taskerl:start_link(true),
    TaskerlPid.

flush() ->
    receive M -> [M | flush()] after 0 -> [] end.


print(Something) ->
    ct:print("~p~n", [Something]).

