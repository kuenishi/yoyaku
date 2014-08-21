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

%% @doc Register Yoyaku to the system. The yoyaku is stored in Riak.
-spec yoyaku:do(Name::atom(), Opaque::any(), After::non_neg_integer(),
                Options::proplists()) -> ok | {error, term()}.
do(_Name, _Opaque, _After, _Options) ->
    error.
