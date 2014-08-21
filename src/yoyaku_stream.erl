%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created : 21 Aug 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku_stream).

-export([name/1, runner_module/1]).

-record(stream, {name, module, bucket_name, options}).

-callback yoyaku_stream:init(Args::list()) -> {ok, State::term()} | {error, term()}.
-callback yoyaku_stream:handle_invoke(Opaque::any(), Option::proplists:proplist(), State::term()) -> ok | retry.
-callback yoyaku_stream:terminate(State::term()) -> ok.

name(#stream{name=Name}) -> Name.
runner_module(#stream{module=Module}) -> Module.
