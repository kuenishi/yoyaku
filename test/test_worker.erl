-module(test_worker).

-behaviour(yoyaku_stream).

-export([init/1, handle_invoke/3, terminate/1]).

init(Args) ->
    {ok, Args}.

handle_invoke(_, _, Args) ->
    {ok, Args}.

terminate(_) ->
    ok.
