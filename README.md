# YOYAKU

Yoyaku is a small but highly available job scheduling library with
built-in worker pool. All schedule data and results are stored in Riak
so that nothing will be lost.

Any job execution library can be built on top of this application.

## API

```erlang
%% register Yoyaku to the system. The yoyaku is stored in Riak.
yoyaku:do(Name::atom(), Opaque::any(), After::time(), Option::proplists())` - 

-callback yoyaku_stream:init(Args::list()) -> {ok, #state{}}.
-callback yoyaku_stream:handle_invoke(Opaque::any(), Option::proplists(), State::#state{}) -> ok | retry.
-callback yoyaku_stream:terminate(#state{}) -> ok.
```

## Configuration

Riak must be configured to use leveldb, as this uses secondary index
to query Riak.

In `app.config` or whatever application configuration, set

```erlang
{yoyaku,
    {streams,
    [
     {stream, garbage_collection, riak_cs_gc, "riak-cs-gc", []},
     {stream, lifecycle_executer, riak_cs_lifecycle, "riak-cs-lifecycle", []}
    ]},
    {concurrency, 16}, %% TODO
    {riak_info, {env, riak_cs}} %% TODO
}
```

## TODO (in far future)

- adaptive resource management and scheduling
- error handling
- periodical execution like cron
