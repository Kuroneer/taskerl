-module(taskerl).

-behaviour(gen_server).

%%% API

-export([
         start_link/0,
         run/2,
         run_async/2
        ]).


%%% BEHAVIOUR API

-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2
        ]).


%%% API

start_link() ->
    taskerl_gen_server_buffer:start_link(?MODULE, [], []).


run(TaskerlPid, Fun) when is_function(Fun) ->
    gen_server:call(TaskerlPid, {run, Fun}).


run_async(TaskerlPid, Fun) when is_function(Fun) ->
    gen_server:cast(TaskerlPid, {run, Fun}).


%%% BEHAVIOUR

init([]) ->
    {ok, undefined}.


handle_call({run, Fun}, _From, State) ->
    {reply, Fun(), State};

handle_call(_Request, _From, State) ->
    {noreply, State}.


handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.

