%%%-------------------------------------------------------------------
%%% Part of taskerl Erlang App
%%% MIT License
%%% Copyright (c) 2019 Jose Maria Perez Ramos
%%%-------------------------------------------------------------------
-module(taskerl_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

all() -> [
          happy_case_async,
          happy_case_sync,
          happy_case_sync_only_ack,
          overflow,
          taskerl_exit,
          taskerl_task_infinity_timeout
         ].

suite() ->
    [{timetrap, {seconds, 30}}].

end_per_testcase(_Case, _Config) ->
    case flush() of
        [] -> ok;
        Messages -> ct:pal("Messages in queue are: ~p", [Messages])
    end,
    ok.


%%====================================================================
%% Test cases
%%====================================================================

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

    WorkerPid ! 2, % Nothing happens, as it's still waiting for 1

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

    QueueLimit = taskerl:get_queue_size(TaskerlPid),

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
    QueueLimit = 5,
    TaskerlPid = create_taskerl_sync_with_response(),

    Self = self(),
    WaitingFun = fun(Expected) -> Self ! Expected, receive Expected -> Expected end end,

    {_, CompletedJobWaitingRef} = spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [1])) end),

    receive 1 -> ok
    after 1000 -> ct:fail("Work not scheduled")
    end,

    NotScheduledWaitings = [ spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [N])) end) || N <- lists:seq(2,QueueLimit) ],

    wait_for_taskerl_to_have_or_fail(TaskerlPid, QueueLimit),

    process_flag(trap_exit, true),
    Signal = shutdown,
    exit(TaskerlPid, Signal),

    [ receive {'DOWN', Ref, process, _, {taskerl, {error, not_scheduled}}} -> ok
      after 1000 -> ct:fail("Missing return value")
      end
      || {_Pid, Ref} <- NotScheduledWaitings ],

    receive {'DOWN', CompletedJobWaitingRef, process, _, {shutdown, _}} -> ok
    after 1000 -> ct:fail("Missing return value")
    end,

    receive {'EXIT', TaskerlPid, Signal} -> ok
    after 1000 -> ct:fail("Missing exit message")
    end,

    ok.

taskerl_task_infinity_timeout(_Config) ->
    {ok, TaskerlPid} = taskerl:start_link(false, 1000, infinity), % Taskerl will wait for completion
    WorkerPid = taskerl:get_worker(TaskerlPid),

    Self = self(),
    WaitingFun = fun(Expected) -> Self ! Expected, receive Expected -> Expected end end,

    {_, CompletedJobWaitingRef} = spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [1])) end),

    receive 1 -> ok
    after 1000 -> ct:fail("Work not scheduled")
    end,

    NotScheduledNum = 4,
    NotScheduledWaitings = [ spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [N])) end) || N <- lists:seq(2,NotScheduledNum+1) ],

    wait_for_taskerl_to_have_or_fail(TaskerlPid, NotScheduledNum + 1),

    process_flag(trap_exit, true),
    Signal = shutdown,
    exit(TaskerlPid, Signal),

    [ receive {'DOWN', Ref, process, _, {taskerl, {error, not_scheduled}}} -> ok
      after 1000 -> ct:fail("Missing return value")
      end
      || {_Pid, Ref} <- NotScheduledWaitings ],

    timer:sleep(200), %% TODO: Use meck
    WorkerPid ! 1,
    receive {'DOWN', CompletedJobWaitingRef, process, _, 1} -> ok
    after 1000 -> ct:fail("Missing return value")
    end,

    receive {'EXIT', TaskerlPid, Signal} -> ok
    after 1000 -> ct:fail("Missing exit message")
    end,

    ok.


%%====================================================================
%% Internal functions
%%====================================================================

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

wait_for_taskerl_to_have_or_fail(TaskerlPid, QueueLimit) ->
    % Wait for all the tasks to be in the queue (this is a poll, TODO: Use meck)
    Limit = 5,
    lists:foldl(fun(_, true) ->
                        true;
                   (N, _) when N == Limit ->
                        ct:fail("Timeout waiting for queue to fill");
                   (_, _) ->
                        case taskerl:get_queue_size(TaskerlPid) of
                            QueueLimit -> true;
                            _ ->
                                timer:sleep(100),
                                false
                        end
                end,
                false,
                lists:seq(1,Limit)
               ).

print(Something) ->
    ct:print("~p~n", [Something]).

