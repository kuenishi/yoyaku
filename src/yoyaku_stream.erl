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
         interval/1,
         options/1, valid_stream/1,
         daemon_name/1,
         worker_name/1]).

-record(stream, {
          name :: atom(),
          module :: module(),
          bucket_name :: string(),
          interval = 900 :: non_neg_integer(), %% batch interval in seconds
          options :: proplists:proplist()
         }).

-type stream() :: #stream{}.
-export_type([stream/0]).

%% @doc Just generates the state, which is immutable. The state will
%% never be chaneted but might kept at multiple processes.
-callback init(Options::list()) ->
    {ok, State::term()} | {error, term()}.

%% @doc invocation handler usually returns `{ok, term()}' even if
%% there are several minor errors. If `{error, term()}' is returned,
%% Yoyaku system takes it as serious, to stop processing the key and
%% does not process several keys at the same timestamp. So are
%% exceptions and errors thrown.
-callback handle_invoke(Opaque::any(), State::term()) -> {ok, Result::term()} | {error, term()}.

%% @doc As all Yoyakus are processed concurrently (and in parallel),
%% this function is required for aggretating all results.
-callback merge(Result::term(), Result::term()) -> Result::term().

%% @doc reporting one batch.
-callback report_batch(Result::term()) -> any().

%% @doc clean up if one process failed.
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

interval(#stream{interval=IntervalSec}) ->
    IntervalSec.

-spec options(stream()) -> proplists:proplist().
options(#stream{options=Options}) -> Options.

valid_stream(Stream) ->
    try
        Module = runner_module(Stream),
        erlang:get_module_info(Module),
        Exports = Module:module_info(exports),
        lists:member({init,1}, Exports)
            andalso lists:member({handle_invoke, 2}, Exports)
            andalso lists:member({merge, 2}, Exports)
            andalso lists:member({report_batch, 1}, Exports)
            andalso lists:member({terminate, 1}, Exports)
    catch _:_ ->
            false
    end.
        
daemon_name(#stream{name=Name}) ->
    concatinate_atom(yoyaku_d, $_, Name).

worker_name(#stream{name=Name}) ->
    concatinate_atom(yoyaku_worker, $_, Name).

concatinate_atom(Lhs, Char, Rhs) ->
    binary_to_atom(list_to_binary([atom_to_list(Lhs),
                                   Char,
                                   atom_to_list(Rhs)]),
                   latin1).
