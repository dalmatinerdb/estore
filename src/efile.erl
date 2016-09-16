%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Project-FiFo UG
%%% @doc
%%% Simple event store using flat files and mostly ordered storage.
%%% All operations are sequential, there is as most one seek for reads.
%%%
%%% We achive this by keeping data in two files, for once the main
%%% store. It allows for events to be slightly out of order given
%%% a certain grace period. Evetns falling beyond that grace period
%%% are placed in a reconsiliation file.
%%%
%%% The tradeoff here is that during reads the reconciliation file will
%%% be fully consumed. This is OK under the assumption that most events
%%% will be arrive mostly ordered.
%%%
%%% In the future we will need to look into compaction when the recon
%%% file grows too big. Another option to deal with it would be to
%%% actively drop events that are too far out of order.
%%%
%%% The file structure is suposed to be simle. Both the recon and the
%%% man event store file follow this guids:
%%% <pre>
%%% |  4   |  8   |  4 | Size |
%%% | Size | Time | ID | Data |
%%% </pre>
%%% The index file is prefixed with:
%%% <pre>
%%% |    2    |      8       |
%%% | Version | Grace Period |
%%% </pre>
%%% Index data is then encoded as following (using second precision):
%%% <pre>
%%% |  8   |     8    | 4  |
%%% | Time | Position | ID |
%%% </pre>
%%% @end
%%% Created : 10 Sep 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(efile).

%% API exports
-export([new/1, new/2, close/1, append/2, fold/5, fold/3, read/3, count/1,
         delete/1]).
-export_type([efile/0, event/0, fold_fun/0]).
-define(OPTS, [raw, binary]).
-define(VSN, 1).
-record(efile, {
          name,
          grace = 0,
          last = undefined,
          estore,
          idx,
          idx_t = undefined :: gb_trees:tree() | undefined,
          no_index = false :: boolean(),
          read_size = 4 * 1024
         }).
-opaque efile() :: #efile{}.

-type event() ::
        {pos_integer(), <<_:32>>, term()}.

-type opt() :: {grace, pos_integer()} | no_index.

-type fold_fun() :: fun((Time  :: pos_integer(),
                         ID    :: <<_:32>>,
                         Event :: term(),
                         AccIn :: any()) -> AccOut :: any()).

-define(TIME_TYPE, unsigned-integer).
-define(POS_TYPE, unsigned-integer).
-define(SIZE_TYPE, unsigned-integer).

%%====================================================================
%% API functions
%%====================================================================

-spec new(string()) -> {ok, efile()}.
new(Name) ->
    new(Name, []).
-spec new(string(), [opt()]) -> {ok, efile()}.
new(Name, Opts) ->
    EFile = apply_opts(#efile{name = Name}, Opts),
    IdxFile = idx(EFile),
    case {filelib:is_file(IdxFile), EFile#efile.no_index} of
        {true, true} ->
            {ok, Idx} = file:open(IdxFile, [read, write | ?OPTS]),
            {ok, Data} = file:read(Idx, 10),
            file:close(Idx),
            Expected = <<?VSN:16, (EFile#efile.grace):64/?TIME_TYPE>>,
            case Data of
                Expected ->
                    {ok, EFile};
                _ ->
                    {error, invalid_index}
            end;
        {true, false} ->
            read_index(EFile);
        _ ->
            {ok, EFile#efile{idx_t = gb_trees:empty()}}
    end.
-spec close(efile()) -> ok.
close(#efile{estore = undefined, idx = undefined}) ->
    ok;
close(#efile{estore = undefined, idx = Idx}) ->
    file:close(Idx);
close(#efile{estore = EStore, idx = Idx}) ->
    file:close(Idx),
    file:close(EStore).

-spec delete(efile() | string()) -> ok.
delete(Name) when is_list(Name); is_binary(Name) ->
    delete(#efile{name = Name});
delete(EFile = #efile{}) ->
    close(EFile),
    file:delete(recon(EFile)),
    file:delete(estore(EFile)),
    file:delete(idx(EFile)).

-spec append(Events :: [event()],
             EFile  :: efile()) ->
                    {ok, efile()}.

append([], EFile) ->
    {ok, EFile};

append(Es, EFile = #efile{estore = undefined}) ->
    {ok, EStore} = file:open(estore(EFile), [write, append | ?OPTS]),
    append(Es, EFile#efile{estore = EStore});

append([{Time, _, _} = E | Es],
       EFile = #efile{last = Last, grace = Grace})
  when is_integer(Time),
       is_integer(Last),
       Time + Grace < Last ->
    {ok, Recon} = file:open(recon(EFile), [write, append | ?OPTS]),
    ok = file:write(Recon, serialize([E])),
    ok = file:close(Recon),
    append(Es, EFile);

append(Es, EFile = #efile{last = Last, estore = EStore}) ->
    {Time, ID, _} = lists:last(Es),
    Max = case Last of
              undefined -> Time;
              _ -> max(Last, Time)
          end,
    {ok, Pos} = file:position(EStore, cur),
    Res = file:write(EStore, serialize(Es)),
    EFile1 = update_idx(EFile, Pos, ID, Last, Max),
    {Res, EFile1}.

-spec read(Start :: pos_integer(),
           End   :: pos_integer(),
           Efile :: efile()) ->
                  {ok, [event()], efile()}.

read(Start, End, EFile) when Start =< End ->
    Fun = fun(Time, ID, Event, Acc) ->
                  [{Time, ID, Event} | Acc]
          end,
    fold(Start, End, Fun, [] , EFile).

-spec fold(Fun   :: fold_fun(),
           Acc   :: any(),
           EFile :: efile()) ->
                  {ok, any(), efile()}.
fold(Fun, Acc, EFile) ->
    fold(0, infinity, Fun, Acc, EFile).

-spec fold(Start :: non_neg_integer(),
           End   :: pos_integer() | infinity,
           Fun   :: fold_fun(),
           Acc   :: any(),
           EFile :: efile()) ->
                  {ok, any(), efile()}.
fold(Start, End, Fun, Acc, EFile)
  when Start =< End->
    {ok, Acc1, EFile1} =
        case file:open(recon(EFile), [read | ?OPTS]) of
            {ok, Recon} ->
                read_recon(Start, End, EFile, Fun, Acc, undefined, [], Recon);
            _ ->
                read_recon(Start, End, EFile, Fun, Acc, <<>>, [], undefined)
        end,
    {ok, Acc1, EFile1}.

-spec count(EFile :: efile()) ->
                   {ok, non_neg_integer(), efile()}.
count(EFile = #efile{idx_t = undefined}) ->
    {ok, EFile1} = read_index(EFile),
    count(EFile1);
count(EFile = #efile{idx_t = T}) ->
    {ok, gb_trees:size(T), EFile}.


%%====================================================================
%% Internal functions
%%====================================================================

read_index(EFile = #efile{read_size = RS}) ->
    IdxFile = idx(EFile),
    {ok, Idx} = file:open(IdxFile, [read, write | ?OPTS]),
    {ok, Data} = file:read(Idx, RS),
    read_index(Idx, EFile#efile{idx_t = gb_trees:empty()}, Data).

read_index(Idx,
           EFile = #efile{grace = Grace, idx_t = _T},
           <<?VSN:16, Grace:64/?TIME_TYPE, Data/binary>>) ->
    read_index_(Idx, EFile, Data);

read_index(_, _, _) ->
    {error, invalid_index}.

read_index_(Idx, EFile = #efile{read_size = RS}, <<>>) ->
    case file:read(Idx, RS) of
        {ok, Data} ->
            read_index_(Idx, EFile, Data);
        eof ->
            {ok, EFile#efile{idx = Idx}}
    end;
read_index_(Idx, EFile = #efile{idx_t = Tree},
            <<Time:64/?TIME_TYPE, Pos:64/?POS_TYPE, _ID:4/binary, Data/binary>>) ->
    Tree1 = gb_trees:insert(Time, Pos, Tree),
    EFile1 = EFile#efile{idx_t = Tree1, last = Time},
    read_index_(Idx, EFile1, Data).

update_idx(EFile = #efile{idx_t = undefined}, Pos, ID, _Old, New) ->
    {ok, EFile1 = #efile{last = Last}} = read_index(EFile),
    update_idx(EFile1, Pos, ID, Last, New);

update_idx(EFile = #efile{idx = undefined}, Pos, ID, Old, New) ->
    {ok, Idx} = file:open(idx(EFile), [read, write | ?OPTS]),
    file:write(Idx, <<?VSN:16, (EFile#efile.grace):64/?TIME_TYPE>>),
    update_idx(EFile#efile{idx = Idx}, Pos, ID, Old, New);

update_idx(EFile = #efile{idx = Idx, idx_t = Tree}, Pos, ID, Old, New)
  when Old =:= undefined orelse New  > Old  ->
    Tree1 = gb_trees:insert(New, Pos, Tree),
    file:write(Idx, <<New:64/?TIME_TYPE, Pos:64/?POS_TYPE, ID:4/binary>>),
    EFile#efile{last = New, idx_t = Tree1};

update_idx(EFile, _Pos, _Old, _ID, _New) ->
    EFile.
-spec read_recon(
        Start :: non_neg_integer(),
        End :: pos_integer() | infinity,
        EFile  :: efile(),
        Fun :: fold_fun(),
        Acc :: term(),
        Data :: undefined | binary(),
        RAcc :: [event()],
        File :: file:fd() | undefined) ->
                        {ok, term(), efile()}.
read_recon(Start, End, EFile = #efile{read_size = ReadSize},
           Fun, Acc, undefined, RAcc, Recon) ->
    case file:read(Recon, ReadSize) of
        {ok, Data} ->
            read_recon(Start, End, EFile, Fun, Acc, Data, RAcc, Recon);
        _ ->
            read_recon(Start, End, EFile, Fun, Acc, <<>>, RAcc, Recon)
    end;

read_recon(Start, End, EFile, Fun, Acc, <<>>, RAcc, undefined) ->
    case file:open(estore(EFile), [read | ?OPTS]) of
        {ok, EStore} ->
            read(Start, End, EFile, Fun, Acc, undefined, RAcc, EStore);
        _ ->
            Res = lists:foldl(fun({RTime, REvent}, AccIn) ->
                                      Fun(RTime, binary_to_term(REvent), AccIn)
                              end, Acc, RAcc),
            {ok, Res, EFile}
    end;

read_recon(Start, End, EFile, Fun, Acc, <<>>, RAcc, Recon) ->
    file:close(Recon),
    read_recon(Start, End, EFile, Fun, Acc, <<>>, RAcc, undefined);


read_recon(Start, End, EFile, Fun, Acc,
           <<Size:32/?SIZE_TYPE, Time:64/?TIME_TYPE, _ID:4/binary, _:Size/binary, R/binary>>,
           RAcc, Recon)
  when Time < Start;
       Time > End ->
    read_recon(Start, End, EFile, Fun, Acc, R, RAcc, Recon);

read_recon(Start, End, EFile, Fun, Acc,
           <<Size:32/?SIZE_TYPE, Time:64/?TIME_TYPE, ID:4/binary, Event:Size/binary, R/binary>>,
           RAcc, Recon) ->
    read_recon(Start, End, EFile, Fun, Acc, R, [{Time, ID, Event} | RAcc],
               Recon);

read_recon(Start, End, EFile, Fun, Acc, Data, RAcc, Recon) ->
    case  file:read(Recon, EFile#efile.read_size) of
        {ok, Data1} ->
            read_recon(Start, End, EFile, Fun, Acc,
                       <<Data/binary, Data1/binary>>,
                       RAcc, Recon);
        eof ->
            read_recon(Start, End, EFile, Fun, Acc, <<>>, RAcc, Recon)
    end.

%% We first check the case where we start at 0, at that point we don't need
%% to consult the index at all
read(0, End, EFile = #efile{read_size = ReadSize},
     Fun, Acc, undefined, RAcc, EStore) ->
    %% We find the first element that is at least
    %% as large as the start time.
    file:position(EStore, 0),
    case file:read(EStore, ReadSize) of
        {ok, Data} ->
            read(0, End, EFile, Fun, Acc, Data, RAcc, EStore);
        _ ->
            Res = lists:foldl(fun({RTime, RID, REvent}, AccIn) ->
                                      Fun(RTime, RID, binary_to_term(REvent),
                                          AccIn)
                              end, Acc, RAcc),
            {ok, Res, EFile}
    end;

read(Start, End, EFile = #efile{idx_t = undefined},
     Fun, Acc, Data, RAcc, EStore) ->
    {ok, EFile1} = read_index(EFile),
    read(Start, End, EFile1, Fun, Acc, Data, RAcc, EStore);

read(Start, End, EFile = #efile{read_size = ReadSize, idx_t = Tree},
     Fun, Acc, undefined, RAcc, EStore) ->
    Iter = gb_trees:iterator_from(Start, Tree),
    case gb_trees:next(Iter) of
        {_, Pos, _} ->
            file:position(EStore, Pos);
        _ ->
            ok
    end,
    case file:read(EStore, ReadSize) of
        {ok, Data} ->
            read(Start, End, EFile, Fun, Acc, Data, RAcc, EStore);
        _ ->
            Res = lists:foldl(fun({RTime, RID, REvent}, AccIn) ->
                                      Fun(RTime, RID, binary_to_term(REvent),
                                          AccIn)
                              end, Acc, RAcc),
            {ok, Res, EFile}
    end;

read(_Start, End, EFile = #efile{grace  = Grace}, Fun, Acc,
     <<_Size:32/?SIZE_TYPE, Time:64/?TIME_TYPE, _/binary>>,
     RAcc, EStore)
  when Time > End + Grace ->
    file:close(EStore),
    %% TODO: read recon file
    Res = lists:foldl(fun({RTime, RID, REvent}, AccIn) ->
                              Fun(RTime, RID, binary_to_term(REvent), AccIn)
                      end, Acc, RAcc),
    {ok, Res, EFile};

read(Start, End, EFile, Fun, Acc,
     <<Size:32/?SIZE_TYPE, Time:64/?TIME_TYPE, _:4/binary, _:Size/binary, R/binary>>,
     RAcc, EStore) when Time < Start ->
    read(Start, End, EFile, Fun, Acc, R, RAcc, EStore);

read(Start, End, EFile, Fun, Acc,
     <<_Size:32/?SIZE_TYPE, Time:64/?TIME_TYPE, _:4/binary, _/binary>> = Data,
     [{RTime, RID, REvent} | RAcc], EStore) when
      Time >= RTime ->
    Acc1 = Fun(RTime, RID, binary_to_term(REvent), Acc),
    read(Start, End, EFile, Fun, Acc1, Data, RAcc, EStore);

read(Start, End, EFile, Fun, Acc,
     <<Size:32/?SIZE_TYPE, Time:64/?TIME_TYPE, ID:4/binary, Event:Size/binary, R/binary>>,
     RAcc, EStore) ->
    Acc1 = Fun(Time, ID, binary_to_term(Event), Acc),
    read(Start, End, EFile, Fun, Acc1, R, RAcc, EStore);

read(Start, End, EFile, Fun, Acc, Data, RAcc, EStore) ->
    case  file:read(EStore, EFile#efile.read_size) of
        {ok, Data1} ->
            read(Start, End, EFile, Fun, Acc, <<Data/binary, Data1/binary>>,
                 RAcc, EStore);
        eof ->
            file:close(EStore),
            {ok, Acc, EFile}
    end.

serialize(Es) ->
    << <<(serialize(Time, ID, Event))/binary>> || {Time, ID, Event} <- Es >>.

serialize(Time, ID, Event) ->
    B = term_to_binary(Event),
    S = byte_size(B),
    <<S:32/?SIZE_TYPE, Time:64/?TIME_TYPE, ID:4/binary, B/binary>>.

apply_opts(EFile, []) ->
    EFile;
apply_opts(EFile, [{grace, Grace} | R]) ->
    apply_opts(EFile#efile{grace = Grace}, R);
apply_opts(EFile, [no_index | R]) ->
    apply_opts(EFile#efile{no_index = true}, R);
apply_opts(EFile, [open | R]) ->
    {ok, EStore} = file:open(estore(EFile),
                             [write, append | ?OPTS]),
    apply_opts(EFile#efile{estore = EStore}, R).

estore(#efile{name = Name}) ->
    Name ++ ".estore".

recon(#efile{name = Name}) ->
    Name ++ ".recon".

idx(#efile{name = Name}) ->
    Name ++ ".idx".
