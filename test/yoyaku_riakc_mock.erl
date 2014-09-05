%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  2 Sep 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(yoyaku_riakc_mock).

-compile(export_all).

-include_lib("riakc/include/riakc.hrl").

init() ->
    catch meck:new(riakc_pb_socket),
    meck:expect(riakc_pb_socket, start_link,
                fun(_, Port) when is_integer(Port) andalso Port > 0 ->
                        case ets:info(?MODULE) of
                            undefined ->
                                ets:new(?MODULE, [public, ordered_set, named_table]);
                            _ ->
                                ok
                        end,
                        {ok, self()}
                end),
    meck:expect(riakc_pb_socket, stop,
                fun(C) when is_pid(C) -> ok end),
    meck:expect(riakc_pb_socket, ping,
                fun(C) when is_pid(C) -> pong end),
    meck:expect(riakc_pb_socket, get_bucket,
                fun(C, Bucket) when is_pid(C) andalso is_binary(Bucket) ->
                        {ok, [{allow_mult, true}]}
                end),
    meck:expect(riakc_pb_socket, put,
                fun(C, Obj) when is_pid(C) ->
                        B = riakc_obj:bucket(Obj),
                        K = riakc_obj:key(Obj),
                        Bin = riakc_obj:get_update_value(Obj),
                        Obj2 = riakc_obj:new_obj(B, K, <<"vclockisdeadbbf">>,
                                                 [{[], Bin}]),
                        %% TODO: try siblings or see vector clocks
                        true = ets:insert(?MODULE, {{B, K}, Obj2}),
                        ok
                end),
    meck:expect(riakc_pb_socket, get,
                fun(C, B, K) when is_pid(C) ->
                        case ets:lookup(?MODULE, {B, K}) of
                            [{{B,K},Obj}] -> {ok, Obj};
                            [] -> {error, notfound}
                        end
                end),
    meck:expect(riakc_pb_socket, delete_obj,
                %% TODO: get & see vector clocks & delete
                fun(C, Obj) when is_pid(C) ->
                        B = riakc_obj:bucket(Obj),
                        K = riakc_obj:key(Obj),
                        %% TODO: try siblings or see vector clocks
                        true = ets:delete(?MODULE, {B, K}),
                        ok
                end),                        
    meck:expect(riakc_pb_socket, get_index_range,
                fun(C, B, <<"$key">>, StartKey, EndKey, Options)
                      when is_pid(C) ->
                        emulate_get_index_range(B, StartKey, EndKey, Options)
                end).

emulate_get_index_range(Bucket, _Start, _End, Options) ->
    case proplists:get_value(max_results, Options) of
        undefined ->
            Objects0 = ets:tab2list(?MODULE),
            Keys = [ Key || {{B, Key}, _} <- Objects0, B =:= Bucket ],
            {ok, ?INDEX_RESULTS{keys=Keys}};
        MaxResults when is_integer(MaxResults) andalso MaxResults > 0 ->
            case proplists:get_value(continuation, Options) of
                undefined ->
                    First = ets:first(?MODULE),
                    BKeys = get_n(?MODULE, First, MaxResults, [First]),
                    Keys = [ Key || {{B, Key}, _} <- Objects0, B =:= Bucket ],
                    Last = case Keys of [] -> <<>>; [H|_] -> H end,
                    {ok, ?INDEX_RESULTS{keys=Keys, continuation=Last}};
                Key0 ->
                    BKeys = get_n(?MODULE, Key0, MaxResults, []),
                    Keys = [ Key || {{B, Key}, _} <- Objects0, B =:= Bucket ],
                    Last = case Keys of [] -> <<>>; [H|_] -> H end,
                    {ok, ?INDEX_RESULTS{keys=Keys, continuation=Last}}
                end
    end.



get_n(_, '$end_of_table', _, Acc) -> lists:reverse(Acc);
get_n(_, _, 0, Acc) -> lists:reverse(Acc);
get_n(Table, Key, N, Acc0) ->
    case ets:next(Table, Key) of
        '$end_of_table' -> lists:reverse(Acc0);
        NextKey -> get_n(Table, NextKey, N-1, [NextKey|Acc0])
    end.

mock_test() ->
    init(),
    B = <<"pocket">>,
    K = <<"burger">>,
    Data = <<"data">>,
    Obj0 = riakc_obj:new(B, K, Data),

    {ok,C} = riakc_pb_socket:start_link(localhost, 8087),
    ok = riakc_pb_socket:put(C, Obj0),
    {ok, Obj1} = riakc_pb_socket:get(C, B, K),
    {ok, Results} = riakc_pb_socket:get_index_range(C, B, <<"$key">>,
                                                    <<"0">>, <<"zzz">>, []),
    [K] = Results?INDEX_RESULTS.keys,
    ok = riakc_pb_socket:delete_obj(C, Obj1),
    ok.
