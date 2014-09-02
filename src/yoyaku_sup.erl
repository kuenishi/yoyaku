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
    ok = yoyaku_config:init_ets(),
    case yoyaku_config:get_all_streams() of
        {ok, Streams} ->
            Supervisors = [stream_to_childspec(Stream) || Stream <- Streams],
            ok = check_all_bucket_props(Streams),
            ok = yoyaku_config:register_streams(Streams),
            {ok, { {one_for_one, 5, 10}, Supervisors} };
        Other ->
            _ = lager:info("no streams defined: ~p", [Other]),
            {error, no_streams}
    end.

stream_to_childspec(Stream) ->
    Name = yoyaku_stream:name(Stream),
    {Name, {yoyaku_d, start_link, [Stream]},
     permanent, 5000, worker, [yoyaku_d]}.

check_all_bucket_props(Streams) ->
    {ok, C} = riakc_pb_socket:start_link(localhost, 8087),
    ok = riakc_pb_socket:ping(C),
    ValidBuckets = lists:filter(fun(Stream) ->
                                        check_bucket_props(C, Stream)
                                end, Streams),
    ok = riakc_pb_socket:stop(C),
    _Buckets = [yoyaku_stream:bucket_name(Stream) || Stream <- Streams],
    case Streams -- ValidBuckets of
        [] -> ok;
        Rest ->
            %% all buckets should be set as allow_mult
            {error, {invalid_bucket_props, Rest}}
    end.

check_bucket_props(C, Stream) ->             
    Bucket = yoyaku_stream:bucket_name(Stream),
    {ok, Props} = riakc_pb_socket:get_bucket(C, Bucket),
    proplists:get_value(allow_mult, Props).
