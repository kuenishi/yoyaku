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

-export([new/1, pop_keys/1, next/1, finished/1]).

-record(scanner, {
          finished = false,
          continuation,
          end_key,
          keys = [],
          stream
         }).


new(Stream) ->
    End = yoyaku:timestamp_key_prefix(),
    Scanner = #scanner{keys=[], end_key=End, stream=Stream},
    next(Scanner).

pop_keys(Scanner0 = #scanner{keys=Keys}) ->
    {Keys, Scanner0#scanner{keys=[]}}.

next(#scanner{finished=true} = S) ->
    {ok, S};
next(#scanner{keys=Keys0, continuation=Cont0, end_key=End, stream=Stream}
     = Scanner0) ->
    Bucket = yoyaku_stream:bucket_name(Stream),
    IndexQueryOptions = [{max_results, 1024}, {continuation, Cont0}],
    {ok, C} = yoyaku_connection:checkout(),
    try
        {ok, C1} = yoyaku_connection:acquire(C),
        case  riakc_pb_socket:get_index_range(C1,
                                              Bucket,
                                              <<"$key">>,
                                              <<"0">>,
                                              End,
                                              IndexQueryOptions) of
            {ok, IndexResults} ->
                lager:debug("~p - ~p (~p) -> ~p", [<<"0">>, End, IndexQueryOptions,
                                                   IndexResults]),

                ?INDEX_RESULTS{keys=Keys, continuation=Cont} = IndexResults,
                Finished = case Cont of
                               undefined -> true;
                               _ -> false
                           end,
                Scanner = Scanner0#scanner{keys=Keys0++Keys,
                                           continuation=Cont,
                                           finished=Finished},
                {ok, Scanner};

            {error, _} = E ->
                E
        end
                
    after 
        ok = yoyaku_connection:checkin(C)
    end.

finished(#scanner{finished=F}) -> F.
