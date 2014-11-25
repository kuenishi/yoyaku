-module(test_worker).

-behaviour(yoyaku_stream).

-export([init/1, handle_invoke/2,
         merge/2, report_batch/1,
         terminate/1]).

init(Args) ->
    {ok, Args}.

handle_invoke(_, _Args) ->
    {ok, 1}.

merge(L, R) -> L+R.

report_batch(N) ->
    lager:info("test batch done: ~p.", [N]).

terminate(_) ->
    ok.
