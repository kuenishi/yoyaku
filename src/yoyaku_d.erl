%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliace with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc The daemon that handles scheduled execution tasks.
%%
-module(yoyaku_d).

-behaviour(gen_fsm).

%% API
-export([start_link/1,
         manual_start/2,
         status/1,
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
          results = [] :: [Opaque::any()],
          timer :: reference()
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

status(Name) ->
    gen_fsm:sync_send_event(Name, status).

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
    Ref = ping_after(Stream),
    _ = lager:debug("here, timeout> ~p <= ~p", [Stream, erlang:read_timer(Ref)]),
    {ok, idle, #state{stream = Stream, timer=Ref}, 0}.

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
idle({waiting, FromPid}, State) ->
    lager:debug("here> ~p", [FromPid]),
    {next_state, idle, maybe_add_worker(FromPid, State)};

idle({finished, FromPid, _, _}, State) ->
    {next_state, idle, maybe_add_worker(FromPid, State)};

idle({failed, FromPid, _}, State) ->
    {next_state, idle, maybe_add_worker(FromPid, State)};

idle(continue, State) -> %% ignore
    {next_state, idle, State};
idle(_Event, State) ->
    %% unexpected _Event
    _ = lager:debug("Unexpected Event: ~p", [_Event]),
    {next_state, idle, State}.

processing(continue, State) ->
    case try_dispatch_task(State) of
        {ok, DispatchedState} ->
            {next_state, processing, DispatchedState};

        {error, no_worker_available} ->
            {next_state, processing, State};

        {finished, FinishedState} ->
            %% Batch finished!!!!
            FreshState = finalize(FinishedState),
            {next_state, idle, FreshState}
    end;

processing({waiting, FromPid}, State0) ->
    State = maybe_add_worker(FromPid, State0),
    continue(),
    {next_state, processing, State};

processing({failed, FromPid, Key, Reason}, State0) ->
    _ = lager:warning("Failed processing key ~p (~p). Skipping.", [Key, Reason]),

    State = maybe_add_worker(FromPid,
                             update_progress(Key, State0)),

    %% worker will soon calls {waiting, pid()} later
    continue(),
    {next_state, processing, State};

processing({finished, FromPid, Key, Results},
           #state{stream = Stream,
                  results = Results0} = State0) ->
    State = update_progress(Key, State0),
    Module = yoyaku_stream:runner_module(Stream),
    AggResults = reduce_all(Module, Results ++ Results0),
    State2 = maybe_add_worker(FromPid,
                              State#state{results=AggResults}),
    %% worker will soon calls {waiting, pid()} later
    continue(),
    {next_state, processing, State2};

processing(_Event, State) ->
    %% unexpected _Event
    _ = lager:error("unexpected 2: ~p", [_Event]),
    {next_state, processing, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
idle({manual_start, _Argv}, _From, State0) ->

    _ = lager:info("manual batch on triggered"),
    case maybe_start_processing(State0) of
        {ok, State} -> {reply, ok, processing, State};
        E -> {reply, E, idle, State0}
    end;
idle(status, _From, State) ->
    Ref = State#state.timer,
    lager:debug("~p, next batch: ~p", [State, erlang:read_timer(Ref)]),
    {reply, {ok, State}, idle, State};

idle(cancel, _From, State) ->
    {redply, {erorr, not_running}, idle, State};

idle(_Event, _From, State) ->
    _ = lager:debug("~p at ~p", [_Event]),
    Reply = {error, unknown_event_state},
    {reply, Reply, idle, State}.

processing({manual_start, _Argv}, _From, State) ->
    Reply = {error, already_running},
    {reply, Reply, processing, State};

processing(status, _From, State) ->
    Ref = State#state.timer,
    lager:debug("now running: ~p ~p", [State, erlang:read_timer(Ref)]),
    {reply, {ok, State}, processing, State};

processing(cancel, _From, State) ->
    Reply = ok,
    _ = lager:info("batch cancelled manually"),
    FreshState = finalize(State),
    {reply, Reply, idle, FreshState};
processing(_Event, _From, State) ->
    Reply = {error, unknown_event_state},
    {reply, Reply, processing, State}.

%% @private
%% @doc gen_fsm:send_all_state_event/2, this function is called to handle
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
%% @doc gen_fsm:sync_send_all_state_event/[2,3], this function is called
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% @private
%% @doc
handle_info(ping, idle, State0 = #state{stream=Stream}) ->
    lager:debug("here ping came> ~p", [Stream]),
    case maybe_start_processing(State0) of
        {ok, State} ->
            continue(),
            {next_state, processing, State};
        {error, not_started} ->
            Ref = ping_after(Stream),
            {next_state, idle, State0#state{timer=Ref}};
        {error, _} = E ->
            _ = lager:error("Batch didn't start: ~p", [E]),
            Ref = ping_after(Stream),
            {next_state, idle, State0#state{timer=Ref}}
    end;

handle_info(_Info, StateName, State) ->
    _ = lager:error("unknown message at ~p: ~p", [StateName, _Info]),
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

-spec try_dispatch_task(#state{}) -> {ok, #state{}} | {ok, {finished, #state{}}} |
                                     {error, no_worker_available}.
try_dispatch_task(#state{waiting=[]} = _) ->
    {error, no_worker_available};
try_dispatch_task(#state{
                     in_progress = InProgress0,
                     queue = Queue0,
                     scanner = Scanner0,
                     waiting = [FromPid|Waiting]} = State0) ->
    
    case queue:out(Queue0) of
        {{value, Key}, Queue} ->
            ok = yoyaku_worker:push_task(FromPid, Key),
            State = State0#state{queue=Queue,
                                 in_progress=[Key|InProgress0],
                                 waiting=Waiting},
            case try_dispatch_task(State) of
                {error, no_worker_available} -> {ok, State};
                Other -> Other
            end;
        
        {empty, Queue0} ->
            case yoyaku_scanner:finished(Scanner0) of
                true -> 
                    State = State0#state{scanner=Scanner0},
                    case InProgress0 of
                        [] ->
                            {finished, State};
                        _ ->
                            %% still some to go
                            {ok, State}
                    end;
                false ->
                    {ok, Scanner} = yoyaku_scanner:next(Scanner0), %% Scanner can't be undefined while processing
                    {Keys, NextScanner} = yoyaku_scanner:pop_keys(Scanner),
                    State = State0#state{queue=queue:from_list(Keys),
                                         scanner=NextScanner},
                    case try_dispatch_task(State) of
                        {error, no_worker_available} -> {ok, State};
                        Other -> Other
                    end;
                {error, _} = E ->
                    lager:debug("Error: ~p", [E]),
                    try_dispatch_task(State0)
            end
    end.

%% @doc finalize the process and return a fresh state
finalize(State = #state{stream=Stream, waiting=Waiting}) ->
    do_report(State),
    Ref = ping_after(Stream),
    #state{stream=Stream, waiting=Waiting, timer=Ref}.

maybe_start_processing(State0 = #state{scanner=undefined,
                                       stream=Stream}) ->
    %% start batching
    case yoyaku_scanner:new(Stream) of
        {ok, Scanner0} -> 
            _ = lager:debug("~p => ~p", [Stream, Scanner0]), 
            {Keys, Scanner} = yoyaku_scanner:pop_keys(Scanner0),
            _ = lager:debug("~p <- ~p", [Keys, Scanner]), 
            Name = yoyaku_stream:name(Stream),
            case Keys of
                [] ->
                    %% No keys to process
                    _ = lager:debug("Batch ~p didn't run because no tasks left.", [Name]),
                    {error, not_started};
                _ ->
                    State = State0#state{queue = queue:from_list(Keys),
                                         scanner = Scanner},
                    _ = lager:info("starting batch ~p: ~p keys.", [Name, length(Keys)]),
                    {ok, State}
            end;
        {error, _} = E ->
            E
    end.

continue() ->
    lager:debug("                  sending continue!!!!!!!!!!!!!!!!!!!!"),
    gen_fsm:send_event(self(), continue).

ping_after(Stream) ->
    case yoyaku_stream:interval(Stream) of
        infinity -> ok;
        IntervalSec ->
            Self = self(),
            erlang:send_after(IntervalSec * 1000, Self, ping)
    end.

do_report(#state{stream=Stream, results=Results}) ->
    Module = yoyaku_stream:runner_module(Stream),
    Name = yoyaku_stream:name(Stream),
    _ = lager:info("batch ~p has finished", [Name]),
    try
        case reduce_all(Module, Results) of
            [] ->
                _ = lager:info("No results for batch");
            [Result] ->
                Module:report_batch(Result)
        end
    catch _:_ ->
            ok
    end.

update_progress(Key, State = #state{in_progress=InProgress0}) ->
    InProgress = lists:delete(Key, InProgress0),
    State#state{in_progress=InProgress}.

maybe_add_worker(Pid, State = #state{waiting = Waiting}) ->
    case lists:member(Pid, Waiting) of
        true -> State;
        false -> State#state{waiting = [Pid|Waiting]}
    end.

reduce_all(_Module, []) -> [];
reduce_all(Module, [H|L]) -> reduce_all(Module, L, H).

reduce_all(_Module, [], Acc) -> [Acc];
reduce_all(Module, [H|L], Acc) ->
    reduce_all(Module, L, Module:merge(H, Acc)).
