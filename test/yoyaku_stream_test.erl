-module(yoyaku_stream_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

yoyaku_stream_test() ->
    Stream = {stream,
              lifecycle_collector, riak_cs_s3_lifecycle,
              "riak-cs-lifecycle", 900, []},
    ?assertEqual([yoyaku_worker_lifecycle_collector_1,
                  yoyaku_worker_lifecycle_collector_2,
                  yoyaku_worker_lifecycle_collector_3,
                  yoyaku_worker_lifecycle_collector_4,
                  yoyaku_worker_lifecycle_collector_5],
                 yoyaku_stream:worker_names(Stream)).
