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
          overflow_hysteresis,
          taskerl_exit,
          taskerl_task_timeout,
          taskerl_task_infinity_timeout,
          serializer_concurrent_calls,
          serializer_exits_infinity_timeout
         ].

suite() ->
    [{timetrap, {seconds, 30}}].

end_per_testcase(_Case, _Config) ->
    case flush() of
        [] -> ok;
        Messages -> ct:pal("Messages in queue are: ~p", [Messages])
    end,
    ok.


-define(RECEIVE_DO_OR_FAIL(M, Do, Format, Args),
        % To avoid having unsafe ret, function declared and executed in the same line
        fun() ->
            receive M = Ret -> _ = Do, Ret
            after 1000 -> ct:fail("~p: Failed to receive ~s. " ++ Format, [?LINE, ??M] ++ Args)
            end
        end()
       ).
-define(RECEIVE_DO_OR_FAIL(M, Do, Format), ?RECEIVE_DO_OR_FAIL(M, Do, Format, [])).
-define(RECEIVE_DO_OR_FAIL(M, Do        ), ?RECEIVE_DO_OR_FAIL(M, Do,     "", [])).
-define(RECEIVE_OR_FAIL(Msg, Format, Args), ?RECEIVE_DO_OR_FAIL(Msg, ok, Format, Args)).
-define(RECEIVE_OR_FAIL(Msg, Format      ), ?RECEIVE_OR_FAIL(Msg, Format, [])).
-define(RECEIVE_OR_FAIL(Msg              ), ?RECEIVE_OR_FAIL(Msg, "")).

-define(RECEIVE_AND_FAIL(M, Format, Args),
        % To avoid having unsafe ret, function declared and executed in the same line
        fun() ->
                receive M = Ret -> ct:fail("~p: Received unexpected ~p = ~s. " ++ Format, [?LINE, Ret, ??M] ++ Args)
                after 200 -> ok
                end
        end()
       ).
-define(RECEIVE_AND_FAIL(Msg, Format), ?RECEIVE_AND_FAIL(Msg, Format, [])).
-define(RECEIVE_AND_FAIL(Msg        ), ?RECEIVE_AND_FAIL(Msg, "")).
-define(RECEIVE_AND_FAIL(           ), ?RECEIVE_AND_FAIL(_)).


%%====================================================================
%% Test cases
%%====================================================================

happy_case_async(_Config) ->
    TaskerlPid = create_taskerl_sync_with_response(),

    Self = self(),
    WaitingFun = fun() -> Self ! {ping, self()}, receive {pong, Self} -> ok end end,

    taskerl:run_async(TaskerlPid, WaitingFun),
    taskerl:run_async(TaskerlPid, WaitingFun),

    ?RECEIVE_DO_OR_FAIL({ping, WorkerPid},
                        [
                         ?RECEIVE_AND_FAIL({ping, _}, "Unserialized work"),
                         WorkerPid ! {pong, Self}
                        ],
                        "Work not scheduled"),

    ?RECEIVE_DO_OR_FAIL({ping, SameWorkerPid}, SameWorkerPid ! {pong, Self}, "Work not scheduled"),

    ok.

happy_case_sync(_Config) ->
    TaskerlPid = create_taskerl_sync_with_response(),

    Self = self(),
    RetValue = "test",
    WaitingFun = fun() -> Self ! {ping, self()}, receive {pong, Self} -> RetValue end end,

    Waiting = [spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun)) end) || _ <- [1,2]],

    {ping, WorkerPid} = ?RECEIVE_DO_OR_FAIL({ping, _},
                                            ?RECEIVE_AND_FAIL({ping, _}, "Unserialized work"),
                                            "Work not scheduled"),
    WorkerPid ! {pong, Self},

    {'DOWN', Ref , process, Pid , RetValue} = ?RECEIVE_OR_FAIL({'DOWN', _, process, _, _}, "Work not succeeded"),
    StillWaiting = lists:delete({Pid, Ref}, Waiting),

    {ping, WorkerPid} = ?RECEIVE_OR_FAIL({ping, _}, "Work not scheduled"),
    WorkerPid ! {pong, Self},

    {'DOWN', Ref2, process, Pid2, RetValue} = ?RECEIVE_OR_FAIL({'DOWN', _, process, _, _}, "Work not succeeded"),
    [] = lists:delete({Pid2, Ref2}, StillWaiting),

    ok.

happy_case_sync_only_ack(_Config) ->
    TaskerlPid = create_taskerl_sync_with_ack(),

    Self = self(),
    [ {taskerl, {ack, N}} = taskerl:run(
                              TaskerlPid,
                              fun() -> Self ! N, receive N -> io:format("Received ~p~n", [N]), ok end end
                             )
      || N <- lists:seq(1,5) ],

    ongoing   = taskerl:get_request_status(TaskerlPid, 1),
    queued    = taskerl:get_request_status(TaskerlPid, 2),
    undefined = taskerl:get_request_status(TaskerlPid, 6),

    WorkerPid = taskerl:get_worker(TaskerlPid),

    WorkerPid ! 2, % Nothing happens, as it's still waiting for 1
    ?RECEIVE_AND_FAIL(2),

    [ ?RECEIVE_DO_OR_FAIL(N, if N < 3 -> WorkerPid ! N; true -> ok end, "Work ~p not scheduled", [N]) || N <- [1,2,3] ],

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
    [ taskerl:run_async(TaskerlPid, WaitingFun, [N]) || N <- lists:seq(1, QueueLimit) ],

    {taskerl, {error, queue_full}} = taskerl:run(TaskerlPid, WaitingFun, [unexpected_work]),
    {taskerl, {error, queue_full}} = taskerl:run(TaskerlPid, WaitingFun, [unexpected_work]),
    ok = taskerl:run_async(TaskerlPid, WaitingFun, [unexpected_work]),

    ?RECEIVE_AND_FAIL(unexpected_work),

    QueueLimit = taskerl:get_queue_size(TaskerlPid),

    queued    = taskerl:get_request_status(TaskerlPid, QueueLimit),
    undefined = taskerl:get_request_status(TaskerlPid, QueueLimit + 1),

    WorkerPid = taskerl:get_worker(TaskerlPid),

    [ ?RECEIVE_DO_OR_FAIL(N, WorkerPid ! N, "Work ~p not scheduled", [N]) || N <- lists:seq(1,QueueLimit) ],
    ?RECEIVE_AND_FAIL(),

    WorkerPid = taskerl:run(TaskerlPid, fun() -> self() end),

    ok.

overflow_hysteresis(_Config) ->
    QueueLimit = 50,
    Hysteresis = 10,
    TaskerlPid = create_taskerl_sync_with_response(QueueLimit, Hysteresis),

    Self = self(),
    WaitingFun = fun(Expected) -> Self ! Expected, receive Expected -> ok end end,
    [ taskerl:run_async(TaskerlPid, WaitingFun, [N]) || N <- lists:seq(1, QueueLimit) ],

    {taskerl, {error, queue_full}} = taskerl:run(TaskerlPid, WaitingFun, [unexpected_work]),
    {taskerl, {error, queue_full}} = taskerl:run(TaskerlPid, WaitingFun, [unexpected_work]),
    ok = taskerl:run_async(TaskerlPid, WaitingFun, [unexpected_work]),

    ?RECEIVE_AND_FAIL(unexpected_work),

    QueueLimit = taskerl:get_queue_size(TaskerlPid),

    queued    = taskerl:get_request_status(TaskerlPid, QueueLimit),
    undefined = taskerl:get_request_status(TaskerlPid, QueueLimit + 1),

    WorkerPid = taskerl:get_worker(TaskerlPid),

    % Complete some work, but not enough for taskerl to go back online
    [ ?RECEIVE_DO_OR_FAIL(N, WorkerPid ! N, "Work ~p not scheduled", [N]) || N <- lists:seq(1, Hysteresis) ],
    Unlocker = Hysteresis + 1,
    ?RECEIVE_DO_OR_FAIL(Unlocker, fun() ->
                                          QueueSize = taskerl:get_queue_size(TaskerlPid),
                                          QueueSize = QueueLimit - Hysteresis,
                                          ongoing = taskerl:get_request_status(TaskerlPid, Unlocker),

                                          % The Hysteresis + 1 work is ongoing
                                          % (counted as queued), sadly there's
                                          % still no room for new works
                                          {taskerl, {error, queue_full}} = taskerl:run(TaskerlPid, WaitingFun, [unexpected_work]),
                                          ok = taskerl:run_async(TaskerlPid, WaitingFun, [unexpected_work]),
                                          ?RECEIVE_AND_FAIL(unexpected_work),
                                          % But eventually, it gets done
                                          WorkerPid ! Unlocker
                                  end(), "Work ~p not scheduled", [Unlocker]),

    % Verify that it's back online once the work is done
    Next = Unlocker + 1,
    ?RECEIVE_DO_OR_FAIL(Next, fun() ->
                                      % After Hysteresis + 1 is completed,
                                      % there's room again
                                      taskerl:run_async(TaskerlPid, WaitingFun, [QueueLimit + 1]),
                                      WorkerPid ! Next
                              end(), "Work ~p not scheduled", [Next]),

    % Complete all the work left
    [ ?RECEIVE_DO_OR_FAIL(N, WorkerPid ! N, "Work ~p not scheduled", [N]) || N <- lists:seq(Next + 1, QueueLimit + 1) ],
    ?RECEIVE_AND_FAIL(),

    WorkerPid = taskerl:run(TaskerlPid, fun() -> self() end),

    ok.

taskerl_exit(_Config) ->
    QueueLimit = 5,
    TaskerlPid = create_taskerl_sync_with_response(),

    Self = self(),
    WaitingFun = fun(Expected) -> Self ! Expected, receive Expected -> Expected end end,

    {_, CompletedJobWaitingRef} = spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [1])) end),

    ?RECEIVE_OR_FAIL(1, "Job started"),

    NotScheduledWaitings = [ spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [N])) end) || N <- lists:seq(2,QueueLimit) ],

    wait_for_taskerl_to_have_or_fail(TaskerlPid, QueueLimit),

    process_flag(trap_exit, true),
    Signal = shutdown,
    exit(TaskerlPid, Signal),
    [ ?RECEIVE_OR_FAIL({'DOWN', Ref, process, _, {taskerl, {error, not_scheduled}}}, "Job not scheduled")
      || {_Pid, Ref} <- NotScheduledWaitings ],
    ?RECEIVE_OR_FAIL({'DOWN', CompletedJobWaitingRef, process, _, {Signal, _}}, "Job finished"),
    ?RECEIVE_OR_FAIL({'EXIT', TaskerlPid, Signal}, "Taskerl exit"),

    ok.

taskerl_task_timeout(_Config) ->
    {ok, TaskerlPid} = taskerl:start_link(false, 1000, 500), % Taskerl will wait for completion for 1s

    Self = self(),
    WaitingFun = fun(Expected) -> Self ! Expected, receive Expected -> Expected end end,

    {_, CompletedJobWaitingRef} = spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [1])) end),

    ?RECEIVE_OR_FAIL(1, "Job started"),

    NotScheduledNum = 4,
    NotScheduledWaitings = [ spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [N])) end) || N <- lists:seq(2,NotScheduledNum+1) ],

    wait_for_taskerl_to_have_or_fail(TaskerlPid, NotScheduledNum + 1),

    process_flag(trap_exit, true),
    Signal = shutdown,
    exit(TaskerlPid, Signal),
    [ ?RECEIVE_OR_FAIL({'DOWN', Ref, process, _, {taskerl, {error, not_scheduled}}}, "Job not scheduled")
      || {_Pid, Ref} <- NotScheduledWaitings ],
    ?RECEIVE_OR_FAIL({'DOWN', CompletedJobWaitingRef, process, _, {Signal, _}}, "Job finished"),
    ?RECEIVE_OR_FAIL({'EXIT', TaskerlPid, Signal}, "Taskerl exit"),

    ok.

taskerl_task_infinity_timeout(_Config) ->
    {ok, TaskerlPid} = taskerl:start_link(false, 1000, infinity), % Taskerl will wait for completion
    WorkerPid = taskerl:get_worker(TaskerlPid),

    Self = self(),
    WaitingFun = fun(Expected) -> Self ! Expected, receive Expected -> Expected end end,

    {_, CompletedJobWaitingRef} = spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [1])) end),

    ?RECEIVE_OR_FAIL(1, "Job started"),

    NotScheduledNum = 4,
    NotScheduledWaitings = [ spawn_monitor(fun() -> exit(taskerl:run(TaskerlPid, WaitingFun, [N])) end) || N <- lists:seq(2,NotScheduledNum+1) ],

    wait_for_taskerl_to_have_or_fail(TaskerlPid, NotScheduledNum + 1),

    process_flag(trap_exit, true),
    Signal = shutdown,
    exit(TaskerlPid, Signal),
    [ ?RECEIVE_OR_FAIL({'DOWN', Ref, process, _, {taskerl, {error, not_scheduled}}}, "Job not scheduled")
      || {_Pid, Ref} <- NotScheduledWaitings ],

    WorkerPid ! 1,

    ?RECEIVE_OR_FAIL({'DOWN', CompletedJobWaitingRef, process, _, 1}, "Job finished"),
    ?RECEIVE_OR_FAIL({'EXIT', TaskerlPid, Signal}, "Taskerl exit"),

    ok.

serializer_concurrent_calls(_Config) ->
    Self = self(),
    Options = [
               {ack_instead_of_reply, true},
               {cast_to_call, true},
               {queue_max_size, 10},
               {max_pending, 2}
              ],
    {ok, TaskerlPid} = taskerl_gen_server_serializer:start_link(?MODULE, Self, [], Options),
    WorkerPid = taskerl:get_worker(TaskerlPid),

    [ begin
          {taskerl, {ack, Msg}} = gen_server:call(TaskerlPid, {store, Msg}),
          ?RECEIVE_OR_FAIL({stored, Msg})
      end || Msg <- lists:seq(1, 2) ],

    [ begin
          {taskerl, {ack, Msg}} = gen_server:call(TaskerlPid, {store, Msg})
      end || Msg <- lists:seq(3, 10) ],

    ?RECEIVE_AND_FAIL({stored, _}),

    {taskerl, {error, queue_full}} = gen_server:call(TaskerlPid, {store, 11}),

    gen_server:call(WorkerPid, reply_last),
    ?RECEIVE_OR_FAIL({stored, 3}, "Worker received message"),
    ?RECEIVE_OR_FAIL({replied, _, 2}, "Worker finished message"),

    ongoing  = taskerl_gen_server_serializer:get_request_status(TaskerlPid, 1),
    finished = taskerl_gen_server_serializer:get_request_status(TaskerlPid, 2),
    ongoing  = taskerl_gen_server_serializer:get_request_status(TaskerlPid, 3),
    queued   = taskerl_gen_server_serializer:get_request_status(TaskerlPid, 4),

    gen_server:call(WorkerPid, reply_last),
    ?RECEIVE_OR_FAIL({stored, 4}, "Worker received message"),
    ?RECEIVE_OR_FAIL({replied, _, 3}, "Worker finished message"),

    finished  = taskerl_gen_server_serializer:get_request_status(TaskerlPid, 3),
    ongoing   = taskerl_gen_server_serializer:get_request_status(TaskerlPid, 4),

    % Taskerl exits even if worker exits with normal
    process_flag(trap_exit, true),
    gen_server:cast(WorkerPid, {exit, normal}),
    ?RECEIVE_OR_FAIL({'EXIT', TaskerlPid, normal}, "Taskerl exit"),

    ok.

serializer_exits_infinity_timeout(_Config) ->
    Options = [{termination_wait_for_current_timeout, infinity}],
    {ok, TaskerlPid} = taskerl_gen_server_serializer:start_link(?MODULE, self(), [], Options),
    WorkerPid = taskerl:get_worker(TaskerlPid),
    process_flag(trap_exit, true),

    % Worker has 1 task
    {_, CompletedJobWaitingRef} = spawn_monitor(fun() -> exit(gen_server:call(TaskerlPid, {store, 1})) end),
    ?RECEIVE_OR_FAIL({stored, 1}, "Worker received message"),
    ongoing = taskerl_gen_server_serializer:get_request_status(TaskerlPid, 1),

    Signal = shutdown,
    exit(TaskerlPid, Signal),
    {taskerl, {error, not_scheduled}} = gen_server:call(TaskerlPid, {store, 2}),

    ?RECEIVE_AND_FAIL(),

    % Worker completes the request
    Ref = monitor(process, WorkerPid),
    gen_server:call(WorkerPid, reply_last),
    ?RECEIVE_OR_FAIL({replied, _, 1}, "Worker finished message"),
    ?RECEIVE_OR_FAIL({'DOWN', CompletedJobWaitingRef, process, _, 1}, "Job finished"),
    % Worker exits
    ?RECEIVE_OR_FAIL({'DOWN', Ref, process, _, Signal}, "Worker exits"),
    ?RECEIVE_OR_FAIL({'EXIT', TaskerlPid, Signal}, "Taskerl exit"),

    ok.


%%====================================================================
%% Internal functions
%%====================================================================

create_taskerl_sync_with_response() ->
    create_taskerl_sync_with_response(1000).

create_taskerl_sync_with_response(QueueLimit) ->
    create_taskerl_sync_with_response(QueueLimit, 0).

create_taskerl_sync_with_response(QueueLimit, Hysteresis) ->
    {ok, TaskerlPid} = taskerl:start_link(false, QueueLimit, 0, Hysteresis),
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


%%====================================================================
%% gen_server callbacks (to run with serializer)
%%====================================================================

init(TesterPid) ->
    process_flag(trap_exit, true),
    {ok, {TesterPid, []}}.

handle_call({store, A}, From, {TesterPid, Values}) ->
    TesterPid ! {stored, A},
    {noreply, {TesterPid, [{From, A} | Values]}};

handle_call(reply_last, _From, {TesterPid, [{OriginalFrom, A} | Values]}) ->
    gen_server:reply(OriginalFrom, A),
    TesterPid ! {replied, OriginalFrom, A},
    {reply, A, {TesterPid, Values}}.

handle_cast({exit, Reason}, State) ->
    {stop, Reason, State};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(Request, {TesterPid, _} = State) ->
    TesterPid ! {info, Request},
    {noreply, State}.

