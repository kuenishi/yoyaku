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
    ets:delete_all_objects(yoyaku_riakc_mock),
    Apps = [yoyaku|apps()],
    [application:stop(A) || A <- Apps].


yoyaku_reserve_test() ->
    prepare_apps(),

    Streams = [{stream, test_stream, test_worker, "test_stream", 10, []}],
    [?assert(yoyaku_stream:valid_stream(Stream)) || Stream <- Streams],
    ok = application:set_env(yoyaku, streams, Streams),
    ok = application:start(yoyaku),
    ok = yoyaku:do(test_stream, foobar, 0, []),

    Keys = get_all_keys(<<"test_stream">>),

    ?assertEqual(1, length(Keys)),
    %% ?debugVal(IndexResult?INDEX_RESULTS.keys),

    terminate_apps().

yoyaku_exec_test() ->
    prepare_apps(),

    Streams = [{stream, test_stream, test_worker, "test_stream", 10, []}],
    [?assert(yoyaku_stream:valid_stream(Stream)) || Stream <- Streams],
    ok = application:set_env(yoyaku, streams, Streams),
    ok = application:start(yoyaku),
    ok = yoyaku:do(test_stream, foobar, 0, []),
    Keys = get_all_keys(<<"test_stream">>),
    ?debugVal(Keys),
    [?debugVal(get_key(<<"test_stream">>, Key)) || Key <- Keys],

    lists:foreach(fun(Stream) ->
                          Name = yoyaku_stream:daemon_name(Stream),
                          yoyaku_d:manual_start(Name, foo)
                  end, Streams),
    yoyaku_d:manual_start(yoyaku_d_test_stream, foo),

    %% wait for keys be swept
    timer:sleep(1000),

    ?debugVal(get_all_keys(<<"test_stream">>)),
    terminate_apps().

get_all_keys(Bucket) ->
    {ok, C} = riakc_pb_socket:start_link(localhost, 8087),
    {ok, IndexResult} = riakc_pb_socket:get_index_range(C, Bucket,
                                                        <<"$key">>,
                                                        <<"0">>, <<"z">>, []),
    ok = riakc_pb_socket:stop(C),

    IndexResult?INDEX_RESULTS.keys.

get_key(Bucket, Key) ->
    {ok, C} = riakc_pb_socket:start_link(localhost, 8087),
    {ok, Result} = riakc_pb_socket:get(C, Bucket, Key),
    ok = riakc_pb_socket:stop(C),
    ?debugVal(riakc_obj:get_contents(Result)),
    Result.
    
