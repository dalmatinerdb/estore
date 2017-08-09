-module(efile_eqc).

-include_lib("eqc/include/eqc.hrl").
-export([prop_comp_file/0,
         prop_fold_file/0]).
-export([new/1, renew/2, append/3]).

-define(DIR, ".qcdata").
-define(M, <<"metric">>).
-compile({no_auto_import,[time/0]}).

time() ->
    ?LET(I, int(), 1470000000000000000 + erlang:abs(estore_eqc:to_nano(I))).

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
    {ok, Store1} = efile:append([{Time, estore:eid(), Event}], Store),
    Store1.

store(Size) ->
    ?LAZY(oneof(
            [{{call, ?MODULE, new,  [?DIR]},
              {call, dict, new, []}} || Size == 0]
            ++ [frequency(
                  [{9, append(Size)},
                   {1, reopen(Size)}]) || Size > 0])).

start_end() ->
    ?SUCHTHAT({S, E}, {time(), time()}, S =< E).

fetch(Start, End, Dict) ->
    L = dict:to_list(Dict),
    lists:sort(
      lists:flatten(
        [E || {T, E} <- L,
              T >= Start,
              T =< End])).

fetch(Dict) ->
    L = dict:to_list(Dict),
    lists:sort(
      lists:flatten(
        [E || {_T, E} <- L])).

prop_comp_file() ->
    ?FORALL({ST, {Start, End}}, {store(), start_end()},
            begin
                os:cmd("rm -r " ++ ?DIR),
                os:cmd("mkdir " ++ ?DIR),
                {Store, Dict} = eval(ST),
                {ok, SR, _S1} = efile:read(Start, End, Store),
                SR1 = lists:sort([{T, E} || {T, _, E} <- SR]),
                efile:close(Store),
                TR = fetch(Start, End, Dict),
                ?WHENFAIL(io:format("~p -> ~p~n~p /=~n~p~n",
                                    [Start, End, SR1, TR]),
                          SR1 == TR)
           end).

prop_fold_file() ->
    ?FORALL(ST, store(),
            begin
                os:cmd("rm -r " ++ ?DIR),
                os:cmd("mkdir " ++ ?DIR),
                {Store, Dict} = eval(ST),
                   Fun = fun(Time, _ID, Event, Acc) ->
                                 [{Time, Event} | Acc]
                         end,
                {ok, SR, _S1} = efile:fold(Fun, [], Store),
                SR1 = lists:sort(SR),
                efile:close(Store),
                TR = fetch(Dict),
                ?WHENFAIL(io:format("~p /=~n~p~n",
                                    [SR1, TR]),
                          SR1 == TR)
           end).
