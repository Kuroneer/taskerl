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
%%%   Multiple ongoing :call (defaults to 1)
%%%
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


-type pending_requests_map() :: #{reference() => {{pid(), reference()} | undefined, pos_integer()}}.

%% gen_server state
-record(st, {
          worker = undefined                        :: undefined | pid(),
          pending_requests = #{}                    :: pending_requests_map(),

          queue = queue:new()                       :: queue:queue(),
          queue_length = 0                          :: non_neg_integer(),
          dropped_overflow = 0                      :: non_neg_integer(),
          next_request_id = 1                       :: pos_integer(),

          % Server options - Configurable via proplist
          ack_instead_of_reply = false              :: boolean(),
          cast_to_call = false                      :: boolean(),
          queue_max_size = 1000                     :: integer(),
          termination_wait_for_current_timeout = 0  :: non_neg_integer() | infinity,
          max_pending = 1                           :: pos_integer()
         }).


%%====================================================================
%% Types
%%====================================================================

-type request_status() :: finished | ongoing | queued | undefined.
-type configuration_option() :: ack_instead_of_reply | cast_to_call |
                                queue_max_size | termination_wait_for_current_timeout |
                                max_pending.


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
       queue_length = QueueLength,
       next_request_id = NextRequestId,
       pending_requests = PendingRequests
      } = sys:get_state(SerializerPid),

    if RequestId >= NextRequestId -> undefined;
       RequestId >= NextRequestId - QueueLength -> queued;
       true ->
           case [ok || {_From, Id} <- maps:values(PendingRequests), RequestId == Id] of
               [] -> finished;
               _ -> ongoing
           end
    end.

-spec get_worker(pid()) -> pid().
get_worker(SerializerPid) ->
    (sys:get_state(SerializerPid))#st.worker.

-spec get_queue_size(pid()) -> non_neg_integer().
get_queue_size(SerializerPid) ->
    #st{
       queue_length = QueueLength,
       pending_requests = PendingRequests
      } = sys:get_state(SerializerPid),
    QueueLength + maps:size(PendingRequests).


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
                               max_pending = MaxPending,
                               queue_length = QueueLength,
                               queue_max_size = MaxSize,
                               dropped_overflow = DroppedOverflow
                              } = State) when QueueLength > 0, QueueLength + MaxPending >= MaxSize ->
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
handle_info({RequestRef, Reply}, #st{pending_requests = PendingRequests} = State) ->
    NewPendingRequests = remove_pending_request(RequestRef, PendingRequests, Reply),
    {noreply, maybe_send_request_to_worker(State#st{pending_requests = NewPendingRequests})};
handle_info({'EXIT', WorkerPid, Reason}, #st{worker = WorkerPid} = State) ->
    % Exit if worker does so regardless of the Reason
    {stop, Reason, State#st{worker = undefined}};
handle_info({'EXIT', _Pid, Reason}, State) when Reason /= normal ->
    {stop, Reason, State};
handle_info(_Info, State) ->
    {noreply, State}.


-spec terminate(term(), #st{}) -> ok.
terminate(Reason, #st{
                     worker = WorkerPid,
                     queue = Queue,
                     pending_requests = PendingRequests,
                     termination_wait_for_current_timeout = TerminationTimeout
                    }) ->
    NumDroppedEarly = reply_not_scheduled(Queue),
    {NumTimeouted, NumDropped} = wait_for_pending(PendingRequests, TerminationTimeout, WorkerPid, NumDroppedEarly),
    case NumDropped of
        0 -> ok;
        _ -> logger:warning("~p (~p): Terminating (~p): Dropping ~p non-started requests",
                            [?MODULE, self(), Reason, NumDropped])
    end,
    case NumTimeouted of
        0 -> ok;
        _ -> logger:error("~p (~p): Terminating (~p): Dropping ~p started requests",
                          [?MODULE, self(), Reason, NumTimeouted])
    end,
    ok.


%%====================================================================
%% Internal functions
%%====================================================================

-spec remove_pending_request(reference(), pending_requests_map(), term()) -> pending_requests_map().
remove_pending_request(RequestRef, PendingRequests, Reply) ->
    case maps:take(RequestRef, PendingRequests) of
        error ->
            PendingRequests;
        {{undefined, _RequestId}, PendingReqWithoutThisOne} ->
            PendingReqWithoutThisOne;
        {{From     , _RequestId}, PendingReqWithoutThisOne} ->
            gen_server:reply(From, Reply),
            PendingReqWithoutThisOne
    end.

-spec maybe_send_request_to_worker(#st{}) -> #st{}.
maybe_send_request_to_worker(#st{
                                worker = Worker,
                                pending_requests = PendingRequests,
                                max_pending = MaxPending,
                                queue = Queue,
                                queue_length = QueueLength
                               } = State) when QueueLength > 0 ->
    case maps:size(PendingRequests) >= MaxPending of
        true ->
            State;
        false ->
            {{value, {Request, From, RequestId}}, QueueWithoutRequest} = queue:out(Queue),
            RequestRef = erlang:make_ref(),
            % Emulate gen_server:call request
            erlang:send(Worker, {'$gen_call', {self(), RequestRef}, Request}, [noconnect]),
            maybe_send_request_to_worker(State#st{
                                           pending_requests = PendingRequests#{RequestRef => {From, RequestId}},
                                           queue = QueueWithoutRequest,
                                           queue_length = QueueLength - 1
                                          })
    end;
maybe_send_request_to_worker(State) ->
    State.

-define(TASKERL_ERR_NOT_SCHEDULED, {taskerl, {error, not_scheduled}}).
-spec reply_not_scheduled(queue:queue()) -> integer().
reply_not_scheduled(Queue) ->
    reply_not_scheduled(Queue, 0).
reply_not_scheduled(Queue, Acc) ->
    case queue:out(Queue) of
        {{value, {_Request, undefined, _RequestId}}, QueueWithoutRequest} ->
            reply_not_scheduled(QueueWithoutRequest, Acc + 1);
        {{value, {_Request, From, _RequestId}}, QueueWithoutRequest} ->
            gen_server:reply(From, ?TASKERL_ERR_NOT_SCHEDULED),
            reply_not_scheduled(QueueWithoutRequest, Acc);
        {empty, _} ->
            Acc
    end.

-spec wait_for_pending(pending_requests_map(), non_neg_integer() | infinity, pid() | undefined, non_neg_integer()) ->
    {non_neg_integer(), non_neg_integer()}.
wait_for_pending(PendingRequests, _Timeout, undefined, NumDropped) ->
    % No worker, ignore timeout, just process all that may be in the inbox
    wait_for_pending(PendingRequests, undefined, 0, undefined, NumDropped);
wait_for_pending(PendingRequests, Timeout, WorkerPid, NumDropped) when Timeout == 0; Timeout == infinity ->
    % When timeout is 0 or infinity, there's no need to involve
    % erlang:send_after
    wait_for_pending(PendingRequests, undefined, Timeout, WorkerPid, NumDropped);
wait_for_pending(PendingRequests, Timeout, WorkerPid, NumDropped) ->
    TerminationRef = make_ref(),
    erlang:send_after(Timeout, self(), {terminate_timeout, TerminationRef}),
    wait_for_pending(PendingRequests, TerminationRef, Timeout, WorkerPid, NumDropped).

wait_for_pending(PendingRequests, TerminationRef, MaxTimeout, Worker, NumDropped) ->
    case maps:size(PendingRequests) of
        0 -> {0, NumDropped};
        _ ->
            receive
                {'$gen_call', From, _Msg} ->
                    gen_server:reply(From, ?TASKERL_ERR_NOT_SCHEDULED),
                    wait_for_pending(PendingRequests, TerminationRef, MaxTimeout, Worker, NumDropped);
                {'$gen_cast', _Msg} ->
                    wait_for_pending(PendingRequests, TerminationRef, MaxTimeout, Worker, NumDropped + 1);
                {RequestRef, Reply} when is_reference(RequestRef) ->
                    NewPendingRequests = remove_pending_request(RequestRef, PendingRequests, Reply),
                    wait_for_pending(NewPendingRequests, TerminationRef, MaxTimeout, Worker, NumDropped);
                {terminate_timeout, TerminationRef} when is_reference(TerminationRef) ->
                    {maps:size(PendingRequests), NumDropped};
                {'EXIT', Worker, _Reason} ->
                    wait_for_pending(PendingRequests, undefined, 0, undefined, NumDropped)
            after MaxTimeout ->
                      {maps:size(PendingRequests), NumDropped}
            end
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
record_key_to_index(max_pending)          -> #st.max_pending;
record_key_to_index(_)                    -> -1.

