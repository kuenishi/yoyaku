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

-export([new/1, pop_keys/1]).

-record(scanner, {
          continuation,
          end_key,
          keys = []
         }).


new(Stream) ->
    Bucket = yoyaku_stream:bucket_name(Stream),
    End = yoyaku:timestamp_key_prefix(),
    IndexQueryOptions = [{max_results, 1024}],
    {ok, IndexResults} = riakc_pb_socket:get_index_range(C,
                                                         Bucket,
                                                         <<"$key">>,
                                                         <<"0">>,
                                                         End,
                                                         IndexQueryOptions),
    ?INDEX_RESULTS{keys=Keys, continuation=Cont} = IndexResults,
    #scanner{keys=Keys, continuation=Cont, end_key=End}.

pop_keys(Scanner0 = #scanner{keys=Keys}) ->
    {Keys, Scanner0#scanner{keys=[]}}.
