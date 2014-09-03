%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%  Yoyaku stream scanner
%%% @end
%%% Created : 21 Aug 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku_d).

-behaviour(gen_fsm).

%% API
-export([start_link/1,
         manual_start/2,
         cancel/1]).

-include_lib("eunit/include/eunit.hrl").

%% gen_fsm states
-export([idle/2, idle/3,
         processing/2, processing/3]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {
          stream :: yoyaku_stream:stream(),
          scanner :: yoyaku_scanner:scanner() | undefined,
          queue = queue:new() :: queue:queue(binary()),
          waiting = [] :: [pid()],
          in_progress = [],
          result :: any()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Stream) ->
    Name = yoyaku_stream:daemon_name(Stream),
    gen_fsm:start_link({local, Name}, ?MODULE, [Stream], []).

manual_start(Name, Argv) ->
    %% gen_fsm:sync_send_event({local, Name}, {manual_start, Argv}).
    gen_fsm:sync_send_event(Name, {manual_start, Argv}).

cancel(Name) ->
    gen_fsm:sync_send_event(Name, cancel).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Stream]) ->
    ping_after(Stream),
    {ok, idle, #state{stream = Stream}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
idle({waiting, FromPid}, #state{waiting = Waiting0} = State0) ->
    ping(),
    {next_state, idle,
     State0#state{waiting=[FromPid|Waiting0]}};

idle(_Event, State) ->
    %% unexpected _Event
    _ = lager:debug("unexpected >>> ~p", [_Event]),
    {next_state, idle, State}.

processing({waiting, FromPid},
           #state{waiting = Waiting0} = State0) ->

    Waiting = case lists:member(FromPid, Waiting0) of
                  true -> %% Just in case
                      Waiting0;
                  false ->
                      [FromPid|Waiting0]
              end,
    State = State0#state{waiting=Waiting},
    ping(),
    {next_state, processing, State};

processing({failed, Key},
             #state{in_progress = InProgress0} = State0) ->
    _ = lager:debug("Failed processing key ~p. Skipping.", [Key]),
    InProgress = lists:delete(Key, InProgress0),
    State = State0#state{in_progress=InProgress},
    %% worker will soon calls {waiting, pid()} later
    {next_state, processing, State};

processing({finished, {Key, Result}},
             #state{in_progress = InProgress0,
                    stream = Stream,
                    result = Result0} = State0) ->
    InProgress = lists:delete(Key, InProgress0),
    Module = yoyaku_stream:runner_module(Stream),
    AggregatedResult = case Result0 of
                           undefined -> Result;
                           _ -> Module:merge(Result0, Result)
                       end,
    State = State0#state{in_progress=InProgress,
                         result=AggregatedResult},
    %% worker will soon calls {waiting, pid()} later
    {next_state, processing, State};

processing(_Event, State) ->
    %% unexpected _Event
    {next_state, processing, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
idle({manual_start, _Argv}, _From,
     State0 = #state{scanner=undefined,
                     stream=Stream}) ->

    Name = yoyaku_stream:name(Stream),
    lager:info("starting batch: ~p (~p)", [_Argv, Name]),

    %% start batching
    Scanner0 = yoyaku_scanner:new(Stream),
    {Keys, Scanner} = yoyaku_scanner:pop_keys(Scanner0),
    case Keys of
        [] ->
            %% No keys to process
            ping_after(Stream),
            {reply, ok, idle, State0};
        _ ->
            State = State0#state{queue = queue:from_list(Keys),
                                 scanner = Scanner},
            {reply, ok, processing, State}
    end;
idle(cancel, _From, State) ->
    {next_state, {erorr, not_running}, idle, State};
idle(_Event, _From, State) ->
    _ = lager:debug("~p at ~p", [_Event]),
    Reply = {error, unknown_event_state},
    {reply, Reply, idle, State}.

processing({manual_start, _Argv}, _From, State) ->
    Reply = {error, already_running},
    {reply, Reply, processing, State};
processing(cancel, _From, State0 = #state{stream=Stream}) ->
    Reply = ok,
    State = State0#state{scanner=undefined, result=undefined},
    ping_after(Stream),
    {reply, Reply, idle, State};
processing(_Event, _From, State) ->
    Reply = {error, unknown_event_state},
    {reply, Reply, processing, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------

handle_info(ping, idle, State0 = #state{scanner=undefined,
                                        stream=Stream}) ->
    %% start batching
    Scanner0 = yoyaku_scanner:new(Stream),
    {Keys, Scanner} = yoyaku_scanner:pop_keys(Scanner0),
    _ = lager:debug("~p <- ~p", [Keys, Scanner]), 
    case Keys of
        [] ->
            %% No keys to process
            ping_after(Stream),
            {next_state, idle, State0};
        _ ->
            State = State0#state{queue = queue:from_list(Keys),
                                 scanner = Scanner},
            ping(),
            {next_state, processing, State}
    end;

handle_info(ping, processing,
            #state{waiting = []} = State0) ->
    ?debugHere,
    {next_state, processing, State0};

handle_info(ping, processing,
            #state{
               in_progress = InProgress0,
               queue = Queue0,
               scanner = Scanner0,
               stream = Stream,
               waiting = [FromPid|Waiting]} = State0) ->

    case queue:out(Queue0) of
        {{value, Key}, Queue} ->
            ok = yoyaku_worker:push_task(FromPid, Key),
            {next_state, processing,
             State0#state{queue=Queue, in_progress=[Key|InProgress0],
                          waiting=Waiting}};
        {empty, Queue0} when Scanner0 =/= undefined ->

            case yoyaku_scanner:next(Scanner0) of
                {ok, finished} ->
                    State = State0#state{scanner=undefined},
                    case InProgress0 of
                        [] ->
                            %% Batch finished!!!!
                            do_report(State),
                            ping_after(Stream),
                            {next_state, idle, State#state{result=undefined}};
                        _ ->
                            %% still some to go
                            {next_state, processing, State}
                    end;
                {ok, Scanner} ->
                    {[Key|Keys], NextScanner} = yoyaku_scanner:pop_keys(Scanner),
                    ok = yoyaku_worker:push_task(FromPid, Key),
                    InProgress = [Key|InProgress0],
                    State = State0#state{queue=queue:from_list(Keys),
                                         scanner=NextScanner,
                                         in_progress=InProgress},
                    {next_state, processing, State}
            end;
        {empty, Queue0} ->
            ping_after(Stream),
            {next_state, idle, State0}
    end;

handle_info(ping, StateName, State) ->
    ping_after(State#state.stream),
    {next_state, StateName, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(normal, _, _) -> ok;
terminate(Reason, StateName, _State) ->
    _ = lager:debug("~p at ~p", [Reason, StateName]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

ping() ->
    erlang:send_after(0, self(), ping).

ping_after(Stream) ->
    case yoyaku_stream:interval(Stream) of
        infinity -> ok;
        IntervalSec ->
            erlang:send_after(IntervalSec * 1000, self(), ping)
    end.

do_report(#state{stream=Stream, result=Result}) ->
    Module = yoyaku_stream:runner_module(Stream),
    try
        Module:report_batch(Result)
    after
        ok
    end.
