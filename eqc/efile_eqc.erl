-module(efile_eqc).

-include_lib("eqc/include/eqc.hrl").
-compile(export_all).

-define(DIR, ".qcdata").
-define(M, <<"metric">>).
-compile({no_auto_import,[time/0]}).

time() ->
    ?LET(I, int(), 1470000000000000000 + erlang:abs(ensure_nano(I))).

store() ->
    ?SIZED(Size, store(Size)).

new(Dir) ->
    {ok, R} = efile:new(Dir ++ "/eqc"),
    R.

reopen(Size) ->
    ?LAZY(
       ?LET({S, T}, store(Size - 1),
            {{call, ?MODULE, renew, [?DIR, S]}, T})).

renew(Dir, Store) ->
    ok = efile:close(Store),
    new(Dir).

append(Size) ->
    ?LAZY(
       ?LET({{S, T}, Time, Event},
            {store(Size - 1), time(), binary()},
            {{call, ?MODULE, append, [Time, Event, S]},
             {call, dict, append, [Time, {Time, Event}, T]}})).

append(Time, Event, Store) ->
    {ok, Store1} = efile:append(Time, Event, Store),
    Store1.

store(Size) ->
    ?LAZY(oneof(
            [{{call, ?MODULE, new,  [?DIR]},
              {call, dict, new, []}} || Size == 0]
            ++ [frequency(
                  [{9, append(Size)},
                   {1, reopen(Size)}]) || Size > 0])).

start_end() ->
    ?SUCHTHAT({S, E}, {time(), time()}, S >= E).

fetch(Start, End, Dict) ->
    L = dict:to_list(Dict),
    lists:sort(
      lists:flatten(
        [E || {T, E} <- L,
              T >= Start,
              T =< End])).

prop_comp_comp() ->
    ?FORALL({ST, {Start, End}}, {store(), start_end()},
            begin
                os:cmd("rm -r " ++ ?DIR),
                os:cmd("mkdir " ++ ?DIR),
                {Store, Dict} = eval(ST),
                {ok, SR, _S1} = efile:read(Start, End, Store),
                SR1 = lists:sort(SR),
                efile:close(Store),
                TR = fetch(Start, End, Dict),
                ?WHENFAIL(io:format("~p /= ~p~n", [SR1, TR]),
                          SR1 == TR)
           end).

ensure_nano(X) when X > 1400000000000000000 ->
    X;
ensure_nano(X) when X > 1400000000000000 ->
    erlang:convert_time_unit(X, micro_seconds, nano_seconds);
ensure_nano(X) ->
    erlang:convert_time_unit(X, seconds, nano_seconds).
