% Part of taskerl Erlang App
% MIT License
% Copyright (c) 2019 Jose Maria Perez Ramos

-module(taskerl).

-behaviour(gen_server).

%%% API

-export([
         start_link/1,
         start_link/2,
         run/2,
         run/3,
         run_async/2,
         run_async/3,
         get_request_status/2,
         get_worker/1
        ]).


%%% BEHAVIOUR API

-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2
        ]).


%%% API

start_link(OnlyAck) ->
    start_link(OnlyAck, 1000).

start_link(OnlyAck, QueueLimit) ->
    Options = [
               {ack_instead_of_reply, OnlyAck},
               {call_to_cast, true},
               {queue_max_size, QueueLimit}
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


%%% BEHAVIOUR

init([]) ->
    {ok, undefined}.


handle_call({run, Fun, Args}, _From, State) ->
    {reply, apply(Fun, Args), State};

handle_call(_Request, _From, State) ->
    {noreply, State}.


handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.

