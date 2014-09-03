%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created : 21 Aug 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku_stream).

-export([name/1, bucket_name/1, runner_module/1,
         options/1, valid_stream/1]).

-record(stream, {
          name :: atom(),
          module :: module(),
          bucket_name :: string(),
          options :: proplists:proplist()
         }).

-type stream() :: #stream{}.
-export_type([stream/0]).

-callback init(Args::list()) ->
    {ok, State::term()} | {error, term()}.
-callback handle_invoke(Opaque::any(), Option::proplists:proplist(), State::term()) -> ok | retry.
-callback terminate(State::term()) -> ok.

-spec name(stream()) -> atom().
name(#stream{name=Name}) -> Name.

-spec bucket_name(stream()) -> binary().
bucket_name(#stream{bucket_name=Bucket}) when is_list(Bucket) ->
    list_to_binary(Bucket);
bucket_name(#stream{bucket_name=Bucket}) when is_binary(Bucket) ->
    Bucket.

-spec runner_module(stream()) -> module().
runner_module(#stream{module=Module}) -> Module.

-spec options(stream()) -> proplists:proplist().
options(#stream{options=Options}) -> Options.

valid_stream(Stream) ->
    try
        Module = runner_module(Stream),
        erlang:get_module_info(Module),
        Exports = Module:module_info(exports),
        lists:member({init,1}, Exports)
            andalso lists:member({handle_invoke, 3}, Exports)
            andalso lists:member({terminate, 1}, Exports)
    catch _:_ ->
            false
    end.
        
