-module(estore_eqc).

-include_lib("eqc/include/eqc.hrl").
-compile(export_all).

-define(DIR, ".qcdata").
-define(M, <<"metric">>).
-compile({no_auto_import,[time/0]}).


time() ->
    ?LET(I, int(), 1470000000000000000 + erlang:abs(to_nano(I))).

pos_int() ->
    ?LET(I, int(), abs(I) + 1).

store(FileSize) ->
    ?SIZED(Size, store(Size, FileSize)).

opts() ->
    oneof(
      [[no_index],
       []]).

new(Dir, FileSize, Opts) ->
    {ok, R} = estore:new(Dir ++ "/eqc", [{file_size, FileSize} | Opts]),
    R.


reopen(Size, FileSize) ->
    ?LAZY(
       ?LET({S, T}, store(Size - 1, FileSize),
            {{call, ?MODULE, renew, [?DIR, S, FileSize, opts()]}, T})).

renew(Dir, Store, FileSize, Opts) ->
    ok = estore:close(Store),
    new(Dir, FileSize, Opts).

append(Size, FileSize) ->
    ?LAZY(
       ?LET({{S, T}, Time, Event},
            {store(Size - 1, FileSize), time(), binary()},
            {{call, ?MODULE, append, [Time, Event, S]},
             {call, dict, append, [Time, {Time, Event}, T]}})).

append(Time, Event, Store) ->
    {ok, Store1} = estore:append([{Time, estore:eid(), Event}], Store),
    Store1.

store(Size, FileSize) ->
    ?LAZY(oneof(
            [{{call, ?MODULE, new,  [?DIR, FileSize, []]},
              {call, dict, new, []}} || Size == 0]
            ++ [frequency(
                  [{9, append(Size, FileSize)},
                   {1, reopen(Size, FileSize)}]) || Size > 0])).

start_end() ->
    ?SUCHTHAT({S, E}, {time(), time()}, S =< E).

fetch(Start, End, Dict) ->
    L = dict:to_list(Dict),
    lists:sort(
      lists:flatten(
        [E || {T, E} <- L,
              T >= Start,
              T =< End])).

file_size() ->
    ?LET(X, pos_int(), to_nano(X)).

to_nano(X) ->
    erlang:convert_time_unit(X, seconds, nano_seconds).

split_range() ->
    ?LET({Start, Count, Size},
         {pos_int(), pos_int(), pos_int()},
         {Start, Count, Size + 1}).
test_splits([], Sum, _) ->
    Sum;
test_splits([{S, E} | R], Sum, Size) ->
    case (S div Size =:= E div Size) of
        false ->
            Sum;
        true ->
            test_splits(R, Sum + (E - S) + 1, Size)
    end.

fetch(Dict) ->
    L = dict:to_list(Dict),
    lists:sort(
      lists:flatten(
        [E || {_T, E} <- L])).


prop_comp_store() ->
    ?FORALL(
       FileSize, file_size(),
       ?FORALL({ST, {Start, End}}, {store(FileSize), start_end()},
               begin
                   os:cmd("rm -r " ++ ?DIR),
                   os:cmd("mkdir " ++ ?DIR),
                   {Store, Dict} = eval(ST),
                   {ok, SR, _S1} = estore:read(Start, End, Store),
                   SR1 = lists:sort([{T, E} || {T, _, E} <- SR]),
                   estore:close(Store),
                   TR = fetch(Start, End, Dict),
                   ?WHENFAIL(io:format("~p /= ~p~n", [SR1, TR]),
                             SR1 == TR)
               end)).

prop_splits() ->
    ?FORALL({Start, Count, Size}, split_range(),
            begin
                Splits = estore:make_splits(Start, Start + Count - 1, Size),
                Sum = test_splits(Splits, 0, Size),
                ?WHENFAIL(io:format("~p: ~p~n", [Sum, Splits]),
                          Sum =:= Count)
            end).

prop_fold_store() ->
    ?FORALL(
       FileSize, file_size(),
       ?FORALL(ST, store(FileSize),
               begin
                   os:cmd("rm -r " ++ ?DIR),
                   os:cmd("mkdir " ++ ?DIR),
                   {Store, Dict} = eval(ST),
                   Fun = fun(Time, _ID, Event, Acc) ->
                                 [{Time, Event} | Acc]
                         end,
                   {ok, SR, _S1} = estore:fold(Fun, [], Store),
                   SR1 = lists:sort(SR),
                   estore:close(Store),
                   TR = fetch(Dict),
                   ?WHENFAIL(io:format("~p /=~n~p~n",
                                       [SR1, TR]),
                             SR1 == TR)
               end)).
