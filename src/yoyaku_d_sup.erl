-module(yoyaku_d_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_link(Stream) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Stream]).

init([Stream]) ->
    DaemonName = yoyaku_stream:daemon_name(Stream),
    DaemonSup = {DaemonName, {yoyaku_d, start_link, [Stream]},
                 permanent, 5000, worker, [yoyaku_d]},

    Workers = [begin
                   WorkerName = yoyaku_stream:worker_name(Stream, I),
                   {WorkerName, {yoyaku_worker, start_link, [Stream, I]},
                    permanent, 5000, worker, [yoyaku_worker]}
               end
               || I <- lists:seq(1, yoyaku_stream:concurrency(Stream)) ], %% TODO: make this as list
    {ok, { {one_for_one, 5, 10}, [DaemonSup] ++ Workers} }.
