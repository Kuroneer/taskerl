% Part of taskerl Erlang App
% MIT License
% Copyright (c) 2019 Jose Maria Perez Ramos

-module(taskerl_gen_server_serializer).

%% Starts and links a gen_server worker with the given values
%% Serializes any gen_server:call() received to that worker with a limit on
%% queued tasks
%%
%% Optional:
%%   Transforms any gen_server:cast to a gen_server:call
%%   Returns an ack as soon as the gen_server:call is queued, instead of waiting
%%   for the reply (the reply is discarded)


-behaviour(gen_server).

%%% API

-export([
         start_link/3,
         start_link/4,
         start_link/5,
         get_request_status/2,
         get_worker/1
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

          queue = queue:new(),
          queue_length = 0,
          dropped_overflow = 0,
          next_request_id = 1,
          last_completed_request_id = 0,

          %% Server options - Configurable via proplist
          ack_instead_of_reply = false,
          call_to_cast = false,
          queue_max_size = 1000
         }).


%%% API

start_link(Module, Args, WorkerOptions) ->
    start_link(Module, Args, WorkerOptions, []).


start_link(Module, Args, WorkerOptions, SerializerOptions) when is_atom(Module) ->
    InitialState = initial_state_from_proplist(SerializerOptions),
    gen_server:start_link(?MODULE, [InitialState, Module, Args, WorkerOptions], []);

start_link(SomeServerName, Module, Args, WorkerOptions) ->
    start_link(SomeServerName, Module, Args, WorkerOptions, []).


start_link({serializer, SerializerServerName}, Module, Args, WorkerOptions, SerializerOptions) ->
    InitialState = initial_state_from_proplist(SerializerOptions),
    gen_server:start_link(SerializerServerName, ?MODULE, [InitialState, Module, Args, WorkerOptions], []);

start_link(WorkerServerName, Module, Args, WorkerOptions, SerializerOptions) ->
    InitialState = initial_state_from_proplist(SerializerOptions),
    gen_server:start_link(?MODULE, [InitialState, WorkerServerName, Module, Args, WorkerOptions], []).


get_request_status(SerializerPid, RequestId) ->
    #st{
       last_completed_request_id = LastCompletedRequestId,
       next_request_id = NextRequestId
      } = sys:get_state(SerializerPid),
    if RequestId =< LastCompletedRequestId -> finished;
       RequestId == LastCompletedRequestId + 1 -> ongoing;
       RequestId <  NextRequestId -> queued;
       true -> undefined
    end.

get_worker(SerializerPid) ->
    (sys:get_state(SerializerPid))#st.worker.

%%% BEHAVIOUR

init([InitialState, ServerName, Module, Args, Options]) ->
    process_flag(trap_exit, true),
    init(gen_server:start_link(ServerName, Module, Args, Options), InitialState);

init([InitialState, Module, Args, Options]) ->
    process_flag(trap_exit, true),
    init(gen_server:start_link(Module, Args, Options), InitialState).

init({ok, WorkerPid}, InitialState) -> {ok, InitialState#st{worker = WorkerPid}};
init({error, Reason}, _)            -> {stop, Reason}.


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
            {reply, {taskerl, {error, queue_full}}, State}
    end;

handle_call(Request, From, #st{dropped_overflow = DroppedOverflow} = State) when DroppedOverflow > 0 ->
    logger:warning("~p (~p): Overflowing stopped: Dropped ~p requests", [?MODULE, self(), DroppedOverflow]),
    handle_call(Request, From, State#st{dropped_overflow = 0});

handle_call(Request, From, #st{
                              ack_instead_of_reply = true,
                              next_request_id = NextRequestId
                             } = State) when From /= undefined ->
    gen_server:reply(From, {taskerl, {ack, NextRequestId}}),
    handle_call(Request, undefined, State);

handle_call(Request, From, #st{
                              queue = Queue,
                              queue_length = QueueLength,
                              next_request_id = NextRequestId
                             } = State) ->
    {noreply, maybe_send_request_to_worker(State#st{
                                             queue = queue:in({Request, From, NextRequestId}, Queue),
                                             queue_length = QueueLength + 1,
                                             next_request_id = NextRequestId + 1
                                            })}.


handle_cast(Request, #st{call_to_cast = true} = State) ->
    handle_call(Request, undefined, State);

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({RequestRef, Reply}, #st{                                   %% gen_server:call reply
                                    request_pending_ref = RequestRef,
                                    queue = Queue,
                                    queue_length = QueueLength
                                   } = State) ->
    {{value, {_Request, From, RequestId}}, QueueWithoutRequest} = queue:out(Queue),
    case From of
        undefined -> ok;
        _ -> gen_server:reply(From, Reply)
    end,
    {noreply, maybe_send_request_to_worker(State#st{
                                             queue = QueueWithoutRequest,
                                             queue_length = QueueLength - 1,
                                             request_pending_ref = undefined,
                                             last_completed_request_id = RequestId
                                            })};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(Reason, #st{queue = Queue, queue_length = QueueLength, worker = Worker}) ->
    exit(Worker, Reason),
    case QueueLength of
        0 ->
            ok;
        _ ->
            NumDropped = reply_not_scheduled(queue:drop(Queue)), %% First request in queue is always in progress
            logger:error("~p (~p): Terminating: Dropping ~p requests", [?MODULE, self(), NumDropped])
    end.


%%% INTERNAL

maybe_send_request_to_worker(#st{
                                queue = Queue,
                                queue_length = QueueLength,
                                request_pending_ref = undefined,
                                worker = Worker
                               } = State) when QueueLength > 0 ->
    {value, {Request, _From, _RequestId}} = queue:peek(Queue),
    RequestRef = erlang:make_ref(),
    erlang:send(Worker, {'$gen_call', {self(), RequestRef}, Request}, [noconnect]), %% Emulate gen_server:call request
    State#st{request_pending_ref = RequestRef};

maybe_send_request_to_worker(State) ->
    State.


reply_not_scheduled( Queue) ->
    reply_not_scheduled(Queue, 0).

reply_not_scheduled( Queue, Acc) ->
    case queue:out(Queue) of
        {{value, {_Request, undefined, _RequestId}}, QueueWithoutRequest} ->
            reply_not_scheduled(QueueWithoutRequest, Acc + 1);
        {{value, {_Request, From, _RequestId}}, QueueWithoutRequest} ->
            gen_server:reply(From, {taskerl, {error, not_scheduled}}),
            reply_not_scheduled(QueueWithoutRequest, Acc);
        {empty, _} ->
            Acc
    end.


%% To initialize the state
initial_state_from_proplist(SerializerOptions) ->
    lists:foldl(fun({Key, Value}, StateIn) ->
                        erlang:setelement(record_key_to_index(Key), StateIn, Value)
                end,
                #st{},
                SerializerOptions
               ).

record_key_to_index(ack_instead_of_reply) -> #st.ack_instead_of_reply;
record_key_to_index(call_to_cast)         -> #st.call_to_cast;
record_key_to_index(queue_max_size)       -> #st.queue_max_size;
record_key_to_index(_)                    -> -1.

