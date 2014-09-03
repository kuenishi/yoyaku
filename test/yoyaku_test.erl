-module(yoyaku_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("riakc/include/riakc.hrl").

yoyaku_startstop_test() ->
    prepare_apps(),
    BadStreams = [
               {stream, garbage_collection, riak_cs_gc, "riak-cs-gc", []},
               {stream, lifecycle_executer, riak_cs_lifecycle, "riak-cs-lifecycle", []}
              ],
    ok = application:set_env(yoyaku, streams, BadStreams),
    {error, _} = application:start(yoyaku),

    ok = application:set_env(yoyaku, streams, []),
    ok = application:start(yoyaku),

    ok = application:stop(yoyaku),

    terminate_apps().


apps() ->
    [riakc, riak_pb, protobuffs, lager, goldrush,
     syntax_tools, compiler].

prepare_apps() ->
    yoyaku_riakc_mock:init(),
    [application:start(A) || A <- lists:reverse(apps())],
    application:load(yoyaku).

terminate_apps() ->
    Apps = [yoyaku|apps()],
    [application:stop(A) || A <- Apps].


yoyaku_reserve_test() ->
    prepare_apps(),

    Streams = [{stream, test_stream, test_worker, "test_stream", []}],
    [?assert(yoyaku_stream:valid_stream(Stream)) || Stream <- Streams],
    ok = application:set_env(yoyaku, streams, Streams),
    ok = application:start(yoyaku),
    ok = yoyaku:do(test_stream, foobar, 0, []),

    {ok, C} = riakc_pb_socket:start_link(localhost, 8087),
    {ok, IndexResult} = riakc_pb_socket:get_index_range(C, <<"test_stream">>,
                                                        <<"$key">>,
                                                        <<"0">>, <<"z">>, []),
    ok = riakc_pb_socket:stop(C),

    ?assertEqual(1, length(IndexResult?INDEX_RESULTS.keys)),
    ?debugVal(IndexResult?INDEX_RESULTS.keys),

    terminate_apps().
