-module(yoyaku_test).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

yoyaku_startstop_test() ->

    ok = application:start(compiler),
    ok = application:start(syntax_tools),
    ok = application:start(goldrush),
    ok = application:start(lager),

    yoyaku_riakc_mock:init(),
    ok = application:start(protobuffs),
    ok = application:start(riak_pb),
    ok = application:start(riakc),

    ok = application:load(yoyaku),
    BadStreams = [
               {stream, garbage_collection, riak_cs_gc, "riak-cs-gc", []},
               {stream, lifecycle_executer, riak_cs_lifecycle, "riak-cs-lifecycle", []}
              ],
    ok = application:set_env(yoyaku, streams, BadStreams),
    {error, _} = application:start(yoyaku),

    ok = application:set_env(yoyaku, streams, []),
    ok = application:start(yoyaku),

    ok = application:stop(yoyaku).
