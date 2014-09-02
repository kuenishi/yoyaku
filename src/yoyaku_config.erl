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
         interval/0]).

init_ets() ->
    true = ets:new(?MODULE, [public, set]),
    ok.

get_all_streams() ->
    case application:get_env(yoyaku, streams) of
        {ok, Streams} when is_list(Streams) ->
            case lists:all(fun yoyaku_stream:valid_stream/1, Streams) of
                true ->
                    {ok, Streams};
                false ->
                    InvalidStream = Streams --
                        lists:filter(fun yoyaku_stream:valid_stream/1, Streams),
                    {error, {invalid_stream, InvalidStream}}
            end;
        Other ->
            Other
    end.

register_streams(Streams) ->
    true = ets:insert(?MODULE, Streams),
    ok.

-spec get_config(atom()) -> {ok, yoyaku_stream:stream()}.
get_config(Name) when is_atom(Name) ->
    case ets:lookup(?MODULE, Name, 2) of
        [Stream] -> {ok, Stream};
        [] -> {error, notfound};
        _ -> {error, {toomany, Name}}
    end.


interval() ->
    90.
