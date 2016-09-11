%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Project-FiFo UG
%%% @doc
%%%
%%% @end
%%% Created : 10 Sep 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(estore).

-export([new/2, close/1, append/2, read/3, make_splits/3]).
-export_type([estore/0]).

-define(VSN, 1).
-define(GRACE, ).

-record(estore, {
          size = 1 :: pos_integer(),
          grace = 0 :: non_neg_integer(),
          file :: efile:efile(),
          chunk :: pos_integer(),
          dir :: string()
         }).
-opaque estore() :: #estore{}.

-type time_unit() ::
        ns | us | ms | s |
        m | h | d | w.
-type tagged_time() ::
        pos_integer() |
        {pos_integer(), time_unit()}.

-type new_opt() ::
        {file_size, tagged_time()} |
        {grace, tagged_time()} .


%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------
-spec new(Dir :: string() | binary(),
          Opts :: [new_opt()]) ->
                 {ok, estore()} |
                 {error, file_size | bad_index}.

new(Dir, Opts) when is_binary(Dir) ->
    new(binary_to_list(Dir), Opts);

new(Dir, Opts) when is_list(Dir), is_list(Opts) ->
    case filelib:is_dir(Dir) of
        true ->
            ok;
        false ->
            ok = file:make_dir(Dir)
    end,
    EStore = apply_opts(#estore{dir = Dir}, Opts),
    open_estore(EStore).

-spec close(estore()) -> ok.
close(EStore) ->
    close_file(EStore),
    ok.

-spec append([efile:event()], estore()) ->
                    {ok, estore()}.
append([], EStore) ->
    EStore;
append([{T, _, _} = E | Es], EStore) ->
    C = chunk(EStore, T),
    write_chunk(EStore, C, [E], Es).

-spec read(Start  :: pos_integer(),
           End    :: pos_integer(),
           Estore :: estore()) ->
                  {ok, [efile:event()], estore()}.

read(Start, End, EStore) when Start =< End->
    Fun = fun(Time, ID, Event, Acc) ->
                  [{Time, ID, Event} | Acc]
          end,
    fold(Start, End, Fun, [] , EStore).

-spec fold(Start  :: pos_integer(),
           End    :: pos_integer(),
           Fun    :: efile:fold_fun(),
           Acc    :: any(),
           EStore :: estore()) ->
                  {ok, any(), estore()}.

fold(Start, End, Fun, Acc, EStore = #estore{size = S}) ->
    Splits = make_splits(Start, End, S),
    {_, Acc1, EStore1} = lists:foldl(fun fold_fun/2, {Fun, Acc, EStore}, Splits),
    {ok, Acc1, EStore1}.

fold_fun({Start, End}, {Fun, Acc, EStore}) ->
    {ok, EStore1}  = open_chunk(EStore, Start),
    {ok, Acc1, File1} = efile:fold(Start, End, Fun, Acc, EStore1#estore.file),
    {Fun, Acc1, EStore1#estore{file = File1}}.

%%====================================================================
%% Private functions.
%%====================================================================    .

write_chunk(EStore, _C, Res, []) ->
    append_sorted(EStore, lists:sort(Res));
write_chunk(EStore, C, Acc, [{Tin, _, _} = E | Es])
  when Tin div EStore#estore.size =:= C ->
    write_chunk(EStore, C, [E | Acc], Es);
write_chunk(EStore, _C, Res, Es) ->
    append_sorted(EStore, lists:sort(Res)),
    append(Es, EStore).


append_sorted(EStore, [{T, _, _} | _] = Es) ->
    {ok, EStore1}  = open_chunk(EStore, T),
    {ok, F1} = efile:append(Es, EStore1#estore.file),
    {ok, EStore1#estore{file = F1}}.

apply_opts(Estore, []) ->
    Estore;
apply_opts(Estore, [{file_size, N} | R])
  when is_integer(N), N > 0 ->
    apply_opts(Estore#estore{size = N}, R);
apply_opts(Estore, [{file_size, N} | R]) ->
    apply_opts(Estore, [{file_size, to_ns(N)} | R]);
apply_opts(Estore, [{grace_period, N} | R])
  when is_integer(N), N > 0 ->
    apply_opts(Estore#estore{grace = N}, R);
apply_opts(Estore, [{grace_period, N} | R]) ->
    apply_opts(Estore, [{grace_period, to_ns(N)} | R]);
apply_opts(Estore, [_ | R]) ->
    apply_opts(Estore, R).

open_chunk(EStore = #estore{size = S, chunk = C}, T)
  when T div S =:= C ->
    {ok, EStore};
open_chunk(EStore = #estore{dir = D, file = undefined, grace = G}, T) ->
    C = chunk(EStore, T),
    File = [D, $/,  integer_to_list(C)],
    {ok, F} = efile:new(File, [{grace, G}]),
    {ok, EStore#estore{chunk = C, file = F}};
open_chunk(EStore, T) ->
    open_chunk(close_file(EStore), T).

close_file(EStore = #estore{file = undefined}) ->
    EStore;
close_file(EStore = #estore{file = F}) when F /= undefined ->
    %% TODO: should we check if the file closes correctly?
    efile:close(F),
    EStore#estore{file = undefined, chunk = undefined}.

chunk(#estore{size = S}, T) ->
    T div S.

open_estore(EStore = #estore{dir = Dir}) ->
    IdxFile = [Dir | "/estore"],
    ExpectedIdx = <<?VSN:16/integer,
                    (EStore#estore.size):64/integer,
                    (EStore#estore.grace):64/integer>>,
    case file:read_file(IdxFile) of
        {ok, ExpectedIdx} ->
            {ok, EStore};
        {ok, <<?VSN:16/integer, _/binary>>} ->
            {error, file_size};
        {ok, _} ->
            {error, bad_index};
        {error,enoent} ->
            file:write_file(IdxFile, ExpectedIdx),
            {ok, EStore}
    end.

-spec to_ns(tagged_time()) ->
                   pos_integer().
to_ns(X) when is_integer(X), X > 0 ->
    X;
to_ns({X, w}) when is_integer(X), X > 0 ->
    erlang:convert_time_unit(X*60*60*24*7, seconds, nano_seconds);
to_ns({X, d}) when is_integer(X), X > 0 ->
    erlang:convert_time_unit(X*60*60*24, seconds, nano_seconds);
to_ns({X, h}) when is_integer(X), X > 0 ->
    erlang:convert_time_unit(X*60*60, seconds, nano_seconds);
to_ns({X, m}) when is_integer(X), X > 0 ->
    erlang:convert_time_unit(X*60, seconds, nano_seconds);
to_ns({X, s}) when is_integer(X), X > 0 ->
    erlang:convert_time_unit(X, seconds, nano_seconds);
to_ns({X, ms}) when is_integer(X), X > 0 ->
    erlang:convert_time_unit(X, milli_seconds, nano_seconds);
to_ns({X, us}) when is_integer(X), X > 0 ->
    erlang:convert_time_unit(X, micro_seconds, nano_seconds);
to_ns({X, ns}) when is_integer(X), X > 0 ->
    X.

make_splits(Time, End, Size)
  when Time div Size =:= End div Size ->
    [{Time, End}];
make_splits(Time, End, Size) ->
    make_splits(Time, End, Size, []).

make_splits(Time, End, Size, Acc)
  when Time div Size =:= End div Size ->
    lists:reverse([{Time, End} | Acc]);
make_splits(Time, End, Size, Acc) ->
    Next = ((Time + Size) div Size) * Size,
    make_splits(Next, End, Size,
                [{Time, Next - 1} | Acc]).
