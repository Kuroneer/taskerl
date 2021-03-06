%%%-------------------------------------------------------------------
%%% Part of taskerl Erlang App
%%% MIT License
%%% Copyright (c) 2019 Jose Maria Perez Ramos
%%%-------------------------------------------------------------------
-module(taskerl).

%% API
-export([
         start_link/0,
         start_link/1,
         start_link/2,
         start_link/3,
         start_link/4,
         run/2,
         run/3,
         run_async/2,
         run_async/3,
         get_request_status/2,
         get_worker/1,
         get_queue_size/1
        ]).

-behaviour(gen_server).
-export([
         init/1,
         handle_call/3,
         handle_cast/2
        ]).


%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    start_link(true).

start_link(OnlyAck) ->
    start_link(OnlyAck, 1000).

start_link(OnlyAck, QueueLimit) ->
    start_link(OnlyAck, QueueLimit, 0).

start_link(OnlyAck, QueueLimit, TerminationTimeout) ->
    start_link(OnlyAck, QueueLimit, TerminationTimeout, 0).

start_link(OnlyAck, QueueLimit, TerminationForCurrentTimeout, Hysteresis) ->
    Options = [
               {ack_instead_of_reply, OnlyAck},
               {cast_to_call, true},
               {queue_max_size, QueueLimit},
               {hysteresis, Hysteresis},
               % Careful with this and the shutdown policy, as it runs in
               % serializer's gen_server:terminate callback
               {termination_wait_for_current_timeout, TerminationForCurrentTimeout}
              ],
    taskerl_gen_server_serializer:start_link(?MODULE, [], [], Options).

run(TaskerlPid, Fun) ->
    run(TaskerlPid, Fun, []).

run(TaskerlPid, Fun, Args) when is_function(Fun) ->
    gen_server:call(TaskerlPid, {run, Fun, Args}).

run_async(TaskerlPid, Fun) ->
    run_async(TaskerlPid, Fun, []).

run_async(TaskerlPid, Fun, Args) when is_function(Fun) ->
    gen_server:cast(TaskerlPid, {run, Fun, Args}).

get_request_status(TaskerlPid, RequestId) ->
    taskerl_gen_server_serializer:get_request_status(TaskerlPid, RequestId).

get_worker(TaskerlPid) ->
    taskerl_gen_server_serializer:get_worker(TaskerlPid).

get_queue_size(TaskerlPid) ->
    taskerl_gen_server_serializer:get_queue_size(TaskerlPid).


%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    {ok, undefined}.

handle_call({run, Fun, Args}, _From, State) ->
    {reply, apply(Fun, Args), State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

