-module(yoyaku_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, Streams} = application:get_env(yoyaku, epics),
    Supervisors = [stream_to_childspec(Stream) || Stream <- Streams],
    {ok, { {one_for_one, 5, 10}, Supervisors} }.

stream_to_childspec(Stream) ->
    Name = yoyaku_stream:name(Stream),
    {Name, {yoyaku_d_sup, start_link, [Stream]},
     permanent, 5000, supervisor, [yoyaku_d_sup]}.
