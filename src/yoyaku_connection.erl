%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%   Yoyaku connection allocator
%%% @end
%%% Created :  3 Sep 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku_connection).

-export([checkout/0, checkin/1]).

-spec checkout() -> {ok, pid()} | {error, term()}.
checkout() ->
    case yoyaku_config:connection_module() of
        {ok, Module} when is_atom(Module) ->
            Module:checkout();
        _ ->
            case yoyaku_config:riak_host() of
                {ok, {Host, Port}} ->
                    riakc_pb_socket:start_link(Host, Port);
                _ ->
                    riakc_pb_socket:start_link(localhost, 8087)
            end
    end.

-spec checkin(pid()) -> ok | {error, term()}.
checkin(Pid) ->
    case yoyaku_config:connection_module() of
        {ok, Module} when is_atom(Module) ->
            Module:checkin(Pid);
        _ ->
            riakc_pb_socket:stop(Pid)
    end.
