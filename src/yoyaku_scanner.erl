%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  2 Sep 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku_scanner).

-include_lib("riakc/include/riakc.hrl").

-export([new/1, pop_keys/1, next/1]).

-record(scanner, {
          continuation,
          end_key,
          keys = [],
          stream
         }).


new(Stream) ->
    Bucket = yoyaku_stream:bucket_name(Stream),
    End = yoyaku:timestamp_key_prefix(),
    IndexQueryOptions = [{max_results, 1024}],
    {ok, C0} = yoyaku_connection:checkout(),
    {ok, C} = riak_cs_riak_client:master_pbc(C0),
    {ok, IndexResults} = riakc_pb_socket:get_index_range(C,
                                                         Bucket,
                                                         <<"$key">>,
                                                         <<"0">>,
                                                         End,
                                                         IndexQueryOptions),
    ?INDEX_RESULTS{keys=Keys, continuation=Cont} = IndexResults,
    lager:debug("~p - ~p -> ~p", [<<"0">>, End, IndexResults]),
    ok = yoyaku_connection:checkin(C0),
    #scanner{keys=Keys, continuation=Cont, end_key=End, stream=Stream}.

pop_keys(Scanner0 = #scanner{keys=Keys}) ->
    {Keys, Scanner0#scanner{keys=[]}}.

next(#scanner{keys=Keys0, continuation=Cont0, end_key=End, stream=Stream}
     = Scanner0) ->
    Bucket = yoyaku_stream:bucket_name(Stream),
    IndexQueryOptions = [{max_results, 1024}, {continuation, Cont0}],
    {ok, C} = yoyaku_connection:checkout(),
    {ok, C1} = riak_cs_riak_client:master_pbc(C),
    {ok, IndexResults} = riakc_pb_socket:get_index_range(C1,
                                                         Bucket,
                                                         <<"$key">>,
                                                         <<"0">>,
                                                         End,
                                                         IndexQueryOptions),
    lager:debug("~p - ~p -> ~p", [<<"0">>, End, IndexResults]),
    ok = yoyaku_connection:checkin(C),
    ?INDEX_RESULTS{keys=Keys, continuation=Cont} = IndexResults,
    case Keys of
        [] -> {ok, finished};
        _ ->
            Scanner = Scanner0#scanner{keys=Keys0++Keys,
                                       continuation=Cont},
            {ok, Scanner}
    end.
