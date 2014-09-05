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

%% @doc Register Yoyaku to the system. The yoyaku is stored in Riak.
-spec yoyaku:do(Name::atom(), Opaque::any(), After::non_neg_integer(),
                Options::proplists:proplist()) -> ok | {error, term()}.
do(Name, Opaque, _After, _Options) ->
    Key = timestamp_key(),
    case yoyaku_config:get_config(Name) of
        {ok, Stream} ->
            Bin = term_to_binary(Opaque),
            Bucket = yoyaku_stream:bucket_name(Stream),
            Obj = riakc_obj:new(Bucket, Key, Bin),
            {ok, C0} = yoyaku_connection:checkout(),
            try
                {ok, C} = riak_cs_riak_client:master_pbc(C0),
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
        {ok, C1} = riak_cs_riak_client:master_pbc(C),
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
    lager:debug(">>>>>>>>>>>>> deleting r_o ~p", [Obj]),
    {ok, C} = yoyaku_connection:checkout(),
    try
        {ok, C1} = riak_cs_riak_client:master_pbc(C),
        riakc_pb_socket:delete_obj(C1, Obj)
    after
        ok = yoyaku_connection:checkin(C)
    end.

timestamp_key() ->
    Second = timestamp(),
    Prefix = integer_to_list(Second),
    _ = random:seed(os:timestamp()),
    Suffix = integer_to_list(random:uniform(100)),
    list_to_binary([Prefix, $_, Suffix]).
    
timestamp() ->
    {MegaSecs, Secs, _MicroSecs} = os:timestamp(),
    (MegaSecs * 1000000) + Secs.

timestamp_key_prefix() ->
    Seconds = timestamp(),
    list_to_binary(integer_to_list(Seconds)).
