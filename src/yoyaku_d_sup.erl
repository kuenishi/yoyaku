-module(yoyaku_d_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_link(Stream) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Stream]).

init(Stream) ->
    DaemonSup = ?CHILD(yoyaku_d, supervisor),
    Worker = {yoyaku_worker, {yoyaku_worker, start_link, [Stream]},
              permanent, 5000, worker, [yoyaku_worker]}, %% TODO: make this as list
    {ok, { {one_for_one, 5, 10}, [DaemonSup, Worker]} }.
