%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created : 21 Aug 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku_worker).

-behaviour(gen_server).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1, push_task/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {stream, state}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Stream) ->
    Name = yoyaku_stream:worker_name(Stream),
    gen_server:start_link({local, Name}, ?MODULE, [Stream], []).

push_task(Pid, Key) ->
    gen_server:call(Pid, {push_task, Key}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Stream]) ->
    Module = yoyaku_stream:runner_module(Stream),
    Options = yoyaku_stream:options(Stream),
    case Module:init(Options) of
        {ok, InternalState} ->
            ping(),
            {ok, #state{stream=Stream,
                        state=InternalState}};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({push_task, Key}, From,
            #state{stream=Stream}=State0) ->
    DaemonName = yoyaku_stream:daemon_name(Stream),
    gen_server:reply(From, ok),
    _ = lager:debug("fetching batch ~p", [Key]),
    case yoyaku:fetch(Stream, Key) of
        {ok, Obj} ->
            Tasks = [binary_to_term(Content) ||
                        {_Meta,Content} <- riakc_obj:get_contents(Obj)],
            _ = lager:debug("task ~p <- ~p", [Tasks, riakc_obj:get_contents(Obj)]),
            case Tasks of
                [] -> 
                    ok = yoyaku:delete(Obj),
                    ok = gen_fsm:send_event(DaemonName, {failed, Key}),
                    ping(),
                    {noreply, State0};
                _ ->
                    case invoke_all_tasks(Tasks, State0, []) of
                        {ok, Result} ->
                            ?debugVal(Result),
                            ok = yoyaku:delete(Obj),
                            ok = gen_fsm:send_event(DaemonName,
                                                    {finished, {Key, Result}}),
                            ping(),
                            {noreply, State0};
                        {error, _} = E ->
                            _ = lager:error("task ~p failed: ~p", [Key, E]),
                            ok = gen_fsm:send_event(DaemonName, {failed, Key}),
                            ping(),
                            {noreply, State0}
                    end
            end;
        {error, _} = E ->
            _ = lager:error("task ~p failed: ~p", [Key, E]),
            ok = gen_fsm:send_event(DaemonName, {failed, Key}),
            ping(),
            {noreply, State0}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(ping, #state{stream=Stream}=State) ->
    DaemonName = yoyaku_stream:daemon_name(Stream),
    Self = self(),
    ok = gen_fsm:send_event(DaemonName, {waiting, Self}),
    {noreply, State};

handle_info(_Info, State) ->
    %% Module:terminate(...),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

ping() ->
    Self = self(),
    erlang:send_after(0, Self, ping).


invoke_all_tasks([], _, [Result]) ->
    {ok, Result};
invoke_all_tasks([Task|Tasks],
                 #state{stream=Stream,
                        state=InternalState0} = State,
                 ResultList) ->
    Module = yoyaku_stream:runner_module(Stream),
    try
        case Module:handle_invoke(Task, InternalState0) of
            {ok, Result} ->
                ResultAcc = lists:foldl(fun Module:merge/2,
                                        Result, ResultList),
                invoke_all_tasks(Tasks, State, [ResultAcc]);
            {error, _} = E ->
                E
        end
    catch Type:Error ->
            {error, {Type, Error}}
    end.
            
