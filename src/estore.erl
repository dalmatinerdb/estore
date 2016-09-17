%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Project-FiFo UG
%%% @doc
%%%  Event store time sharded over mitleople efiles.
%%% @end
%%% Created : 10 Sep 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(estore).

-export([open/1, open/2, new/2, close/1, append/2, read/3, make_splits/3,
         event/1, eid/1, eid/0, fold/3, count/1, delete/1, delete/2]).
-export_type([estore/0]).

-define(VSN, 1).
-define(GRACE, ).

-record(estore, {
          size = 1 :: pos_integer(),
          grace = 0 :: non_neg_integer(),
          file :: efile:efile(),
          chunk :: pos_integer(),
          dir :: string(),
          no_index = false :: boolean()
         }).
-opaque estore() :: #estore{}.

-type time_unit() ::
        ns | us | ms | s |
        m | h | d | w.
-type tagged_time() ::
        pos_integer() |
        {pos_integer(), time_unit()}.

-type new_opt() ::
        %% We don't read the index file ahead of time
        no_index |
        %% The time for each time
        {file_size, tagged_time()} |
        %% The grace period.
        {grace, tagged_time()} .


%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec open(Dir :: string() | binary()) ->
                  {ok, estore()} |
                  {error, file_size | bad_index | enoent}.
open(Dir) ->
    open(Dir, []).

-spec open(Dir :: string() | binary(),
           Opts :: [new_opt()]) ->
                  {ok, estore()} |
                  {error, file_size | bad_index | enoent}.
open(Dir, Opts) when is_binary(Dir) ->
    open(binary_to_list(Dir), Opts);
open(Dir, Opts) ->
    case filelib:is_file(Dir ++ "/estore") of
        false ->
            {error, enoent};
        true ->
            EStore = apply_opts(#estore{dir = Dir}, Opts),
            open_estore(EStore, open)
    end.

%%--------------------------------------------------------------------
%% @doc Creates a new estore, the efiles will be stored in the given
%%   directory.
%% @end
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
    open_estore(EStore, create).


%%--------------------------------------------------------------------
%% @doc Closes all the files in an event store.
%% @end
%%--------------------------------------------------------------------
-spec close(estore()) -> ok.
close(EStore) ->
    close_file(EStore),
    ok.

%%--------------------------------------------------------------------
%% @doc Inserts one or more events into a event store. The events
%%   are expected to be ordered! Sharding will be done automatically
%%   and does not need to be handled outside of the estore.
%%
%%   As in fold strict order is not guarantted.
%% @end
%%--------------------------------------------------------------------
-spec append([efile:event()], estore()) ->
                    {ok, estore()}.
append([], EStore) ->
    EStore;
append([{T, _, _} = E | Es], EStore) ->
    C = chunk(EStore, T),
    write_chunk(EStore, C, [E], Es).

%%--------------------------------------------------------------------
%% @doc Reads a time range from the estore, it will traverse multiple
%%   efiles if required. Keep in mind that this will create one
%%   big aggregator for all events using fold/5 might be a better
%%   choice for larger datasets.
%%
%%   As in fold strict order is not guarantted.
%% @end
%%--------------------------------------------------------------------
-spec read(Start  :: pos_integer(),
           End    :: pos_integer(),
           Estore :: estore()) ->
                  {ok, [efile:event()], estore()}.

read(Start, End, EStore) when Start =< End->
    Fun = fun(Time, ID, Event, Acc) ->
                  [{Time, ID, Event} | Acc]
          end,
    fold(Start, End, Fun, [] , EStore).

%%--------------------------------------------------------------------
%% @doc Folds over a time range, passing each event into the fold
%%   function. Strict order is not guarantted. Events might come out
%%   of order but will remain in the grace period.
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @doc Folds the entire store, passing each event into the fold
%%   function. Strict order is not guarantted. Events might come out
%%   of order but will remain in the grace period.
%% @end
%%--------------------------------------------------------------------
-spec fold(Fun    :: efile:fold_fun(),
           Acc    :: any(),
           EStore :: estore()) ->
                  {ok, any(), estore()}.
fold(Fun, Acc, EStore) ->
    Files = files(EStore),
    Res = lists:foldl(fun (File, AccIn) ->
                              F = efile(File, EStore),
                              {ok, AccOut, F1} = efile:fold(Fun, AccIn, F),
                              efile:close(F1),
                              AccOut
                      end, Acc, Files),
    {ok, Res, EStore}.


%%--------------------------------------------------------------------
%% @doc Returns an APPROXIMATE count of elements in the estore.
%%    This uses the index as an indicator and by that should return
%%    a ballpark of how many entries exist.
%% @end
%%--------------------------------------------------------------------
-spec count(estore()) ->
                   {ok, non_neg_integer(), estore()}.
count(EStore) ->
    Files = files(EStore),
    Res = lists:foldl(fun (File, AccIn) ->
                              F = efile(File, EStore),
                              {ok, N, F1} = efile:count(F),
                              efile:close(F1),
                              AccIn + N
                      end, 0, Files),
    {ok, Res, EStore}.

%%--------------------------------------------------------------------
%% @doc Deletes the store and all files within it.
%% @end
%%--------------------------------------------------------------------
-spec delete(estore()) -> ok.
delete(EStore = #estore{dir = Dir}) ->
    close(EStore),
    Files = files(EStore),
    lists:foldl(fun (File, _) ->
                        efile:delete(File)
                end, ok, Files),
    file:delete([Dir, $/, "estore"]),
    file:del_dir(Dir),
    ok.

%%--------------------------------------------------------------------
%% @doc Delete all efiles that start before a certain point in time.
%% this means some events that existed before 'Before' can still be
%% in there.
%% @end
%%--------------------------------------------------------------------
-spec delete(non_neg_integer(), estore()) ->
                    {ok, estore()}.
delete(Before, EStore = #estore{size = Size})
  when is_integer(Before),
       Before >= 0 ->
    EStore1 = close_file(EStore),
    %% We substract the Size from Before so we can make sure that we
    %% can guarantee everything after Before is still there.
    Files = files(Before - Size, EStore1),
    lists:foldl(fun (File, _) ->
                        efile:delete(File)
                end, ok, Files),
    {ok, EStore1}.

%%--------------------------------------------------------------------
%% @doc Utility function that generates a event id. This is the same
%%   as {@link eid/1} with the argument estore.
%% @end
%%--------------------------------------------------------------------
-spec eid() -> <<_:32>>.
eid() ->
    eid(estore).

%%--------------------------------------------------------------------
%% @doc Creates a event id, using a sha1 hash on the node, the pid,
%%    erlang:unique_integer and a passed term.
%% @end
%%--------------------------------------------------------------------
-spec eid(term()) -> <<_:32>>.
eid(E) ->
    H = erlang:phash2({node(),
                       erlang:unique_integer(),
                       self(),
                       E}),
    <<H:32/unsigned-integer>>.

%%--------------------------------------------------------------------
%% @doc Creates a event tuple with timestamp, an eid and the evnt
%%    passed. The timestamp does NOT have to be provided this function
%%    calls {@link erlang:system_time/1} to get it.
%% @end
%%--------------------------------------------------------------------
event(E) ->
    {erlang:system_time(nano_seconds), eid(), E}.

%%--------------------------------------------------------------------
%% @doc Splits a start end end based into chunks based on the file
%%    sized. This is a utility function that can be used for
%%    distributing estore files.
%% @end
%%--------------------------------------------------------------------
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

%%====================================================================
%% Private functions.
%%====================================================================    .


fold_fun({Start, End}, {Fun, Acc, EStore}) ->
    {ok, EStore1}  = open_chunk(EStore, Start),
    {ok, Acc1, File1} = efile:fold(Start, End, Fun, Acc, EStore1#estore.file),
    {Fun, Acc1, EStore1#estore{file = File1}}.

write_chunk(EStore, _C, Res, []) ->
    append_sorted(EStore, lists:sort(Res));
write_chunk(EStore, C, Acc, [{Tin, _, _} = E | Es])
  when Tin div EStore#estore.size =:= C ->
    write_chunk(EStore, C, [E | Acc], Es);
write_chunk(EStore, _C, Res, Es) ->
    {ok, EStore1} = append_sorted(EStore, lists:sort(Res)),
    append(Es, EStore1).


append_sorted(EStore, [{T, _, _} | _] = Es) ->
    {ok, EStore1}  = open_chunk(EStore, T),
    {ok, F1} = efile:append(Es, EStore1#estore.file),
    {ok, EStore1#estore{file = F1}}.

apply_opts(Estore, []) ->
    Estore;
apply_opts(Estore, [no_index | R]) ->
    apply_opts(Estore#estore{no_index = true}, R);
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
open_chunk(EStore = #estore{dir = D, file = undefined}, T) ->
    C = chunk(EStore, T),
    File = [D, $/,  integer_to_list(C)],
    F = efile(File, EStore),
    {ok, EStore#estore{chunk = C, file = F}};

open_chunk(EStore, T) ->
    open_chunk(close_file(EStore), T).

efile(File, #estore{grace = G, no_index = true}) ->
    {ok, F} = efile:new(File, [{grace, G}, no_index]),
    F;
efile(File, #estore{grace = G}) ->
    {ok, F} = efile:new(File, [{grace, G}]),
    F.

close_file(EStore = #estore{file = undefined}) ->
    EStore;
close_file(EStore = #estore{file = F}) when F /= undefined ->
    %% TODO: should we check if the file closes correctly?
    efile:close(F),
    EStore#estore{file = undefined, chunk = undefined}.

chunk(#estore{size = S}, T) ->
    T div S.

open_estore(EStore = #estore{dir = Dir}, create) ->
    IdxFile = [Dir | "/estore"],
    ExpectedIdx = <<?VSN:16/integer,
                    (EStore#estore.size):64/integer,
                    (EStore#estore.grace):64/integer>>,
    case file:read_file(IdxFile) of
        {ok, ExpectedIdx} ->
            {ok, EStore};
        %% If we create and a file exists with different settings we fail
        {ok, <<?VSN:16/integer, _/binary>>} ->
            {error, file_size};
        %% If the index file could not be read we fail
        {ok, _} ->
            {error, bad_index};
        %% If the file doens't exist we careate it
        {error,enoent} ->
            file:write_file(IdxFile, ExpectedIdx),
            {ok, EStore}
    end;

open_estore(EStore = #estore{dir = Dir}, open) ->
    IdxFile = [Dir | "/estore"],
    case file:read_file(IdxFile) of
        %% If we open we simply overwrite the index with the old value
        {ok, <<?VSN:16/integer, Size:64/integer, Grace:64/integer>>} ->
            EStore1 = apply_opts(EStore, [{file_size, Size}, {grace, Grace}]),
            {ok, EStore1};
        %% If the index file could not be read we fail
        {ok, _} ->
            {error, bad_index};
        %% If the file is not found we can't open.
        {error,enoent} ->
            {error,enoent}
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
files(E) ->
    files(infinity, E).

files(Before, #estore{dir = D, size = Size}) ->
    Fs = filelib:wildcard("*.idx", D),
    Fs1 = lists:sort([file_base(F) || F <- Fs]),
    Fs2 = [F || F <- Fs1, F * Size < Before],
    Fs3 = [D ++ "/" ++ integer_to_list(F) ||
              F <- Fs2],
    Fs3.

file_base(F) ->
    list_to_integer(string:substr(F, 1, length(F) - 4)).
