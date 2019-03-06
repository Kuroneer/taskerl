-module(taskerl_gen_server_buffer).

%% Starts and links a gen_server worker with the given values
%% Sends any gen_server:call() to that worker serialized
%% Transforms any gen_server:cast to a gen_server:call and applies the previous

%% TODO
%% Allow 'stored' mode, which answers all the calls() with 'stored' as soon as
%% they are received, not after the response has been issued


-behaviour(gen_server).

%%% API

-export([
         start_link/3,
         start_link/4
        ]).


%%% BEHAVIOUR API

-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2
        ]).


%%% SERVER STATE

-record(st, {
          worker = undefined,
          request_pending_ref = undefined,

          queue = undefined,
          queue_length = 0,

          dropped_overflow = 0,
          %%TODO Make this value configurable
          queue_max_size = 1000
         }).


%%% API

%% TODO Allow Tasker Options
start_link(Module, Args, WorkerOptions) ->
    gen_server:start_link(?MODULE, [Module, Args, WorkerOptions], []).


start_link(WorkerServerName, Module, Args, WorkerOptions) ->
    gen_server:start_link(?MODULE, [WorkerServerName, Module, Args, WorkerOptions], []).


%%% BEHAVIOUR

init([ServerName, Module, Args, Options]) ->
    init(gen_server:start_link(ServerName, Module, Args, Options));

init([Module, Args, Options]) ->
    init(gen_server:start_link(Module, Args, Options));

init({ok, WorkerPid}) ->
    process_flag(trap_exit, true),
    {ok, #st{
            queue = queue:new(),
            queue_length = 0,
            worker = WorkerPid
           }};

init({error, Reason}) ->
    {stop, Reason}.


handle_call(_Request, From, #st{
                              queue_length = QueueLength,
                              queue_max_size = MaxSize,
                              dropped_overflow = DroppedOverflow
                             } = State) when QueueLength >= MaxSize ->
    case {From, DroppedOverflow} of
        {undefined, 0} ->
            logger:warning("~p (~p): Overflowing: Dropping requests...", [?MODULE, self()]),
            {noreply, State#st{dropped_overflow = 1}};
        {undefined, _} ->
            {noreply, State#st{dropped_overflow = DroppedOverflow + 1}};
        _ ->
            {reply, {error, {taskerl, queue_full}}, State}
    end;

handle_call(Request, From, #st{dropped_overflow = DroppedOverflow} = State) when DroppedOverflow > 0 ->
    logger:warning("~p (~p): Overflowing stopped: Dropped ~p requests", [?MODULE, self(), DroppedOverflow]),
    handle_call(Request, From, State#st{dropped_overflow = 0});

%%TODO
% handle_call(Request, From, #st{mode = stored_ack} = State) when From /= undefined ->
%     gen_server:reply(From, {taskerl, stored_ack}),
%     handle_call(Request, undefined, State);

handle_call(Request, From, #st{
                              queue = Queue,
                              queue_length = QueueLength
                             } = State) ->
    {noreply, maybe_send_request_to_worker(State#st{
                                             queue = queue:in({Request, From}, Queue),
                                             queue_length = QueueLength + 1
                                            })}.


handle_cast(Request, State) ->
    handle_call(Request, undefined, State).


handle_info({RequestRef, Reply}, #st{
                                    request_pending_ref = RequestRef,
                                    queue = Queue,
                                    queue_length = QueueLength
                                   } = State) ->
    {{value, {_Request, From}}, QueueWithoutRequest} = queue:out(Queue),
    case From of
        undefined -> ok;
        _ -> gen_server:reply(From, Reply)
    end,
    {noreply, maybe_send_request_to_worker(State#st{
                                             queue = QueueWithoutRequest,
                                             queue_length = QueueLength - 1,
                                             request_pending_ref = undefined
                                            })};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(Reason, #st{queue_length = QueueLength, worker = Worker}) ->
    %%TODO let callers know if the request has been scheduled or not
    exit(Worker, Reason),
    case QueueLength of
        0 -> ok;
        N -> logger:error("~p (~p): Terminating: Dropping ~p requests", [?MODULE, self(), N])
    end.


%%% INTERNAL

maybe_send_request_to_worker(#st{
                                queue = Queue,
                                queue_length = QueueLength,
                                request_pending_ref = undefined,
                                worker = Worker
                               } = State) when QueueLength > 0 ->
    {value, {Request, _From}} = queue:peek(Queue),
    RequestRef = erlang:make_ref(),
    erlang:send(Worker, {'$gen_call', {self(), RequestRef}, Request}, [noconnect]),
    State#st{request_pending_ref = RequestRef};

maybe_send_request_to_worker(State) ->
    State.

