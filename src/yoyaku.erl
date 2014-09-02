%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created : 21 Aug 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku).

-export([do/4]).

-export([timestamp_key_prefix/0]).

%% @doc Register Yoyaku to the system. The yoyaku is stored in Riak.
-spec yoyaku:do(Name::atom(), Opaque::any(), After::non_neg_integer(),
                Options::proplists:proplist()) -> ok | {error, term()}.
do(Name, Opaque, _After, _Options) ->
    {ok, C} = riakc_pb_socket:start_link(localhost, 8087),
    Key = timestamp_key(),
    case yoyaku_config:get_config(Name) of
        {ok, Stream} ->
            Bin = term_to_binary(Opaque),
            Bucket = yoyaku_stream:bucket_name(Stream),
            ok = riakc_pb_socket:put(C, Bucket, Key, Bin),
            ok = riakc_pb_socket:stop(C);
        Error ->
            Error
    end.


timestamp_key() ->
    Second = timestamp(),
    Prefix = integer_to_list(Second),
    _ = random:seed(Second),
    Suffix = random:uniform(100),
    list_to_binary([Prefix, $_, Suffix]).
    
timestamp() ->
    {MegaSecs, Secs, _MicroSecs} = os:timestamp(),
    (MegaSecs * 1000000) + Secs.

timestamp_key_prefix() ->
    Seconds = timestamp(),
    list_to_binary(integer_to_list(Seconds)).
