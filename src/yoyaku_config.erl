%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  2 Sep 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku_config).

-export([init_ets/0,
         get_all_streams/0,
         register_streams/1,
         get_config/1,
         connection_module/0,
         riak_host/0
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

init_ets() ->
    ?MODULE = ets:new(?MODULE, [public, bag, named_table]),
    ok.

get_all_streams() ->
    case application:get_env(yoyaku, streams) of
        {ok, Streams} when is_list(Streams) ->
            case lists:all(fun yoyaku_stream:valid_stream/1, Streams) of
                true ->
                    {ok, Streams};
                false ->
                    ValidStreams = lists:filter(fun yoyaku_stream:valid_stream/1, Streams),
                    InvalidStream = Streams -- ValidStreams,
                    {error, {invalid_stream, InvalidStream}}
            end;
        Other ->
            Other
    end.

register_streams(Streams0) ->
    Streams = [{yoyaku_stream:name(Stream), Stream} || Stream <- Streams0],
    true = ets:insert(?MODULE, Streams),
    ok.

-spec get_config(atom()) -> {ok, yoyaku_stream:stream()}.
get_config(Name) when is_atom(Name) ->
    try
        case ets:lookup(?MODULE, Name) of
            [{Name,Stream}] -> {ok, Stream};
            [] -> {error, notfound};
            _ -> {error, {toomany, Name}}
        end
    catch Type:Error ->
            lager:error("yoyaku is not enabled (~p:~p)", [Type, Error]),
            {error, badarg}
    end.

connection_module() ->
    application:get_env(yoyaku, connection_module).

riak_host() ->
    application:get_env(yoyaku, riak_host).


-ifdef(TEST).
get_all_streams_test() ->
    Streams0 = [{stream,test_stream,test_worker,"test_stream",10,[]}],
    ok = application:set_env(yoyaku, streams, Streams0),
    ?assertEqual({ok, Streams0}, get_all_streams()).

-endif.
