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

%% for internal use
-export([timestamp_key_prefix/0,
         fetch/2, delete/1]).

-export([manual_start/0, status/0]).

%% @doc Register Yoyaku to the system. The yoyaku is stored in Riak.
-spec yoyaku:do(Name::atom(), Opaque::any(), AfterSec::non_neg_integer(),
                Options::proplists:proplist()) -> ok | {error, term()}.
do(Name, Opaque, AfterSec, _Options) when is_integer(AfterSec)
                                          andalso AfterSec > 0 ->
    Key = timestamp_key(AfterSec),

    case yoyaku_config:get_config(Name) of
        {ok, Stream} ->
            Bin = term_to_binary(Opaque),
            Bucket = yoyaku_stream:bucket_name(Stream),
            Obj = riakc_obj:new(Bucket, Key, Bin),
            {ok, C0} = yoyaku_connection:checkout(),
            try
                {ok, C} = yoyaku_connection:acquire(C0),
                riakc_pb_socket:put(C, Obj)
            after
                ok = yoyaku_connection:checkin(C0)
            end;
        Error ->
            Error
    end.

-spec fetch(yoyaku_stream:stream(), Key::binary()) -> {ok, riakc_obj:riakc_obj()} | {error, term()}.
fetch(Stream, Key) ->
    Bucket = yoyaku_stream:bucket_name(Stream),
    {ok, C} = yoyaku_connection:checkout(),
    try
        {ok, C1} = yoyaku_connection:acquire(C),
        lager:debug(">>>>>>>>>>>>> fetching r_o ~p ~p", [Bucket, Key]),
        case riakc_pb_socket:get(C1, Bucket, Key) of
            {ok, Obj} ->
                {ok, Obj};
            {error, _} = E ->
                E
        end
    after     
        ok = yoyaku_connection:checkin(C)
    end.

-spec delete(riakc_obj:riakc_obj()) -> ok | {error, term()}.
delete(Obj) ->
    lager:debug(">>>>>>>>>>>>> deleting r_o ~p", [riakc_obj:key(Obj)]),
    {ok, C} = yoyaku_connection:checkout(),
    try
        {ok, C1} = yoyaku_connection:acquire(C),
        riakc_pb_socket:delete_obj(C1, Obj)
    after
        ok = yoyaku_connection:checkin(C)
    end.

timestamp_key(AfterSec0) ->
    Second = timestamp(),
    AfterSec = maybe_test_mode(AfterSec0),
    Prefix = integer_to_list(Second + AfterSec),
    _ = random:seed(os:timestamp()),
    Suffix = integer_to_list(random:uniform(100)),
    list_to_binary([Prefix, $_, Suffix]).

maybe_test_mode(AfterSec) ->
    case application:get_env(yoyaku, test_mode) of
        true ->
            case AfterSec div 10000 of
                0 -> 1;
                S -> S
            end;
        _ ->
            AfterSec
    end.

timestamp() ->
    {MegaSecs, Secs, _MicroSecs} = os:timestamp(),
    (MegaSecs * 1000000) + Secs.

timestamp_key_prefix() ->
    Seconds = timestamp(),
    list_to_binary(integer_to_list(Seconds)).

manual_start() ->
    {ok, Streams} = yoyaku_config:get_all_streams(),
    [begin
         Name = yoyaku_stream:daemon_name(Stream),
         yoyaku_d:manual_start(Name, [])
     end || Stream <- Streams].

status() ->
    {ok, Streams} = yoyaku_config:get_all_streams(),
    [begin
         Name = yoyaku_stream:daemon_name(Stream),
         {ok, Status} = yoyaku_d:status(Name),
         Status
     end || Stream <- Streams].
