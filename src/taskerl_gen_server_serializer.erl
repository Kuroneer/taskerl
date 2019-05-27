%%%-------------------------------------------------------------------
%%% Starts and links a gen_server worker with the given values
%%% Serializes any gen_server:call() received to that worker with a limit on
%%% queued tasks
%%%
%%% Optional:
%%%   Transforms any gen_server:cast to a gen_server:call
%%%   Returns an ack as soon as the gen_server:call is queued, instead of waiting
%%%   for the reply (the reply is discarded)
%%%   Wait for some time to complete current task on terminate
%%%   Max queue size (defaults to 1000)
%%%-------------------------------------------------------------------
%%% Part of taskerl Erlang App
%%% MIT License
%%% Copyright (c) 2019 Jose Maria Perez Ramos
%%%-------------------------------------------------------------------
-module(taskerl_gen_server_serializer).

%% API
-export([
         start_link/3,
         start_link/4,
         start_link/5,
         get_request_status/2,
         get_queue_size/1,
         get_worker/1
        ]).

-behaviour(gen_server).
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2
        ]).

%% gen_server state
-record(st, {
          worker = undefined                        :: undefined | pid(),
          request_pending_ref = undefined           :: undefined | reference(),

          queue = queue:new()                       :: queue:queue(),
          queue_length = 0                          :: non_neg_integer(),
          dropped_overflow = 0                      :: non_neg_integer(),
          next_request_id = 1                       :: pos_integer(),
          last_completed_request_id = 0             :: non_neg_integer(),

          % Server options - Configurable via proplist
          ack_instead_of_reply = false              :: boolean(),
          cast_to_call = false                      :: boolean(),
          queue_max_size = 1000                     :: integer(),
          termination_wait_for_current_timeout = 0  :: integer() | infinity
         }).


%%====================================================================
%% Types
%%====================================================================

-type request_status() :: finished | ongoing | queued | undefined.
-type configuration_option() :: ack_instead_of_reply | cast_to_call |
                                queue_max_size | termination_wait_for_current_timeout.


%%====================================================================
%% API functions
%%====================================================================

-spec start_link(atom(), list(), list()) -> {ok, pid()} | {error, term()}.
start_link(Module, Args, WorkerOptions) ->
    start_link(Module, Args, WorkerOptions, []).

-spec start_link(atom() | tuple(), list(), list(), list()) -> {ok, pid()} | {error, term()}.
start_link(Module, Args, WorkerOptions, SerializerOptions) when is_atom(Module) ->
    InitialState = initial_state_from_proplist(SerializerOptions),
    gen_server:start_link(?MODULE, [InitialState, Module, Args, WorkerOptions], []);
start_link(SomeServerName, Module, Args, WorkerOptions) ->
    start_link(SomeServerName, Module, Args, WorkerOptions, []).

-spec start_link(tuple(), atom(), list(), list(), list()) -> {ok, pid()} | {error, term()}.
start_link({serializer, SerializerServerName}, Module, Args, WorkerOptions, SerializerOptions) ->
    InitialState = initial_state_from_proplist(SerializerOptions),
    gen_server:start_link(SerializerServerName, ?MODULE, [InitialState, Module, Args, WorkerOptions], []);
start_link(WorkerServerName, Module, Args, WorkerOptions, SerializerOptions) ->
    InitialState = initial_state_from_proplist(SerializerOptions),
    gen_server:start_link(?MODULE, [InitialState, WorkerServerName, Module, Args, WorkerOptions], []).

-spec get_request_status(pid(), integer()) -> request_status().
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

-spec get_worker(pid()) -> pid().
get_worker(SerializerPid) ->
    (sys:get_state(SerializerPid))#st.worker.

-spec get_queue_size(pid()) -> non_neg_integer().
get_queue_size(SerializerPid) ->
    (sys:get_state(SerializerPid))#st.queue_length.


%%====================================================================
%% gen_server callbacks
%%====================================================================

-spec init(list()) -> {ok, #st{}} | {stop, term()}.
init([InitialState, ServerName, Module, Args, Options]) ->
    process_flag(trap_exit, true),
    init(gen_server:start_link(ServerName, Module, Args, Options), InitialState);
init([InitialState, Module, Args, Options]) ->
    process_flag(trap_exit, true),
    init(gen_server:start_link(Module, Args, Options), InitialState).
init({ok, WorkerPid}, InitialState) -> {ok, InitialState#st{worker = WorkerPid}};
init({error, Reason}, _)            -> {stop, Reason}.

-spec handle_call(term(), {pid(), reference()} | undefined, #st{}) -> {reply, {taskerl, term()}, #st{}} | {noreply, #st{}}.
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

-spec handle_cast(term(), #st{}) -> {noreply, #st{}}.
handle_cast(Request, #st{cast_to_call = true} = State) ->
    handle_call(Request, undefined, State);
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(term(), #st{}) -> {noreply, #st{}}.
handle_info({RequestRef, Reply}, #st{ % gen_server:call reply
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


-spec terminate(term(), #st{}) -> ok.
terminate(_Reason, #st{queue_length = 0}) ->
    ok;
terminate( Reason, #st{
                      queue = Queue,
                      request_pending_ref = RequestRef,
                      termination_wait_for_current_timeout = TerminationForCurrentTimeout
                     }) ->
    % First request in queue is always in progress
    {{value, {_Request, From, _RequestId}}, QueueWithoutRequest} = queue:out(Queue),

    case reply_not_scheduled(QueueWithoutRequest) of
        0 ->
            ok;
        NumDropped ->
            logger:warning("~p (~p): Terminating (~w): Dropping ~p non-started requests",
                           [?MODULE, self(), Reason, NumDropped]
                          )
    end,

    receive {RequestRef, Reply} ->
                case From of
                    undefined -> ok;
                    _ -> gen_server:reply(From, Reply)
                end
    after TerminationForCurrentTimeout ->
              logger:error("~p (~p): Terminating (~w): Dropping started request", [?MODULE, self(), Reason])
    end,
    ok.


%%====================================================================
%% Internal functions
%%====================================================================

-spec maybe_send_request_to_worker(#st{}) -> #st{}.
maybe_send_request_to_worker(#st{
                                queue = Queue,
                                queue_length = QueueLength,
                                request_pending_ref = undefined,
                                worker = Worker
                               } = State) when QueueLength > 0 ->
    {value, {Request, _From, _RequestId}} = queue:peek(Queue),
    RequestRef = erlang:make_ref(),
    erlang:send(Worker, {'$gen_call', {self(), RequestRef}, Request}, [noconnect]), % Emulate gen_server:call request
    State#st{request_pending_ref = RequestRef};
maybe_send_request_to_worker(State) ->
    State.

-spec reply_not_scheduled(queue:queue()) -> integer().
reply_not_scheduled(Queue) ->
    reply_not_scheduled(Queue, 0).
reply_not_scheduled(Queue, Acc) ->
    case queue:out(Queue) of
        {{value, {_Request, undefined, _RequestId}}, QueueWithoutRequest} ->
            reply_not_scheduled(QueueWithoutRequest, Acc + 1);
        {{value, {_Request, From, _RequestId}}, QueueWithoutRequest} ->
            gen_server:reply(From, {taskerl, {error, not_scheduled}}),
            reply_not_scheduled(QueueWithoutRequest, Acc);
        {empty, _} ->
            Acc
    end.

-spec initial_state_from_proplist(list({configuration_option(), term()})) -> #st{}.
initial_state_from_proplist(SerializerOptions) ->
    lists:foldl(fun({Key, Value}, StateIn) ->
                        erlang:setelement(record_key_to_index(Key), StateIn, Value)
                end,
                #st{},
                SerializerOptions
               ).

record_key_to_index(ack_instead_of_reply) -> #st.ack_instead_of_reply;
record_key_to_index(cast_to_call)         -> #st.cast_to_call;
record_key_to_index(queue_max_size)       -> #st.queue_max_size;
record_key_to_index(termination_wait_for_current_timeout)  -> #st.termination_wait_for_current_timeout;
record_key_to_index(_)                    -> -1.

