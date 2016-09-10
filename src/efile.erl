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
%%%
%%% |  8   |  16  | Size |
%%% | Size | Time | Data |
%%%
%%% The index file is prefixed with:
%%%
%%% |    2    |      16      |
%%% | Version | Grace Period |
%%%
%%% Index data is then encoded as following:
%%%
%%% |  16  |     8    |
%%% | Time | Position |
%%% @end
%%% Created : 10 Sep 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(efile).

%% API exports
-export([new/1, new/2, close/1, append_ordered/2, append/3, fold/5, read/3]).
-export_type([efile/0, event/0]).
-define(OPTS, [raw, binary]).
-define(VSN, 1).
-record(efile, {
          estore,
          recon,
          idx,
          idx_t = gb_trees:empty(),
          last = undefined,
          grace = 0,
          name,
          read_size = 4 * 1024
         }).
-opaque efile() :: #efile{}.

-type event() ::
        {pos_integer(), term()}.
-type opt() :: {grace, pos_integer()}.


%%====================================================================
%% API functions
%%====================================================================

-spec new(string()) -> {ok, efile()}.
new(Name) ->
    new(Name, []).
-spec new(string(), [opt()]) -> {ok, efile()}.
new(Name, Opts) ->
    EFile = #efile{read_size = RS} = apply_opts(#efile{name = Name}, Opts),
    IdxFile = idx(EFile),
    case filelib:is_file(IdxFile) of
        true ->
            {ok, Idx} = file:open(IdxFile, [read, write | ?OPTS]),
            {ok, Data} = file:read(Idx, RS),
            read_index(Idx, EFile, Data);
        _ ->
            {ok, EFile}
    end.
-spec close(efile()) -> ok.
close(#efile{estore = undefined, idx = undefined}) ->
    ok;
close(#efile{estore = undefined, idx = Idx}) ->
    file:close(Idx);
close(#efile{estore = EStore, idx = Idx}) ->
    file:close(Idx),
    file:close(EStore).

append_ordered([], EFile) ->
    {ok, EFile};

append_ordered(Es, EFile = #efile{estore = undefined}) ->
    {ok, EStore} = file:open(estore(EFile), [write, append | ?OPTS]),
    append_ordered(Es, EFile#efile{estore = EStore});

append_ordered([{Time, Event} | Es], EFile = #efile{last = Last, grace = Grace})
  when is_integer(Time),
       is_integer(Last),
       Time + Grace < Last ->
    {ok, Recon} = file:open(recon(EFile), [write, append | ?OPTS]),
    ok = file:write(Recon, serialize([{Time, Event}])),
    ok = file:close(Recon),
    append_ordered(Es, EFile);

append_ordered(Es, EFile = #efile{last = Last, estore = EStore}) ->
    {Time, _} = lists:last(Es),
    Max = case Last of
              undefined -> Time;
              _ -> max(Last, Time)
          end,
    {ok, Pos} = file:position(EStore, cur),
    Res = file:write(EStore, serialize(Es)),
    EFile1 = update_idx(EFile, Pos, Last, Max),
    {Res, EFile1}.


append(Time, Event, EFile) ->
    append_ordered([{Time, Event}], EFile).

read(Start, End, EFile) ->
    Fun = fun(Time, Event, Acc) ->
                  [{Time, Event} | Acc]
          end,
    fold(Start, End, Fun, [] , EFile).

fold(Start, End, Fun, Acc, EFile) ->
    Acc1 =
        case file:open(recon(EFile), [read | ?OPTS]) of
            {ok, Recon} ->
                read_recon(Start, End, EFile, Fun, Acc, undefined, [], Recon);
            _ ->
                read_recon(Start, End, EFile, Fun, Acc, <<>>, [], undefined)
        end,
    {ok, Acc1, EFile}.

%%====================================================================
%% Internal functions
%%====================================================================

read_index(Idx,
           EFile = #efile{grace = Grace},
           <<?VSN:16, Grace:128/integer, Data/binary>>) ->
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
            <<Time:128/integer, Pos:64/integer, Data/binary>>) ->
    Tree1 = gb_trees:insert(Time, Pos, Tree),
    read_index_(Idx, EFile#efile{idx_t = Tree1, last = Time}, Data).

update_idx(EFile = #efile{idx = undefined}, Pos, Old, New) ->
    {ok, Idx} = file:open(idx(EFile), [read, write | ?OPTS]),
    file:write(Idx, <<?VSN:16, (EFile#efile.grace):128/integer>>),
    update_idx(EFile#efile{idx = Idx}, Pos, Old, New);

update_idx(EFile = #efile{idx = Idx, idx_t = Tree}, Pos, Old, New)
  when New > Old; Old =:= undefined ->
    Tree1 = gb_trees:insert(New, Pos, Tree),
    EFile1 = EFile#efile{last = New, idx_t = Tree1},
    file:write(Idx, <<New:128/integer, Pos:64/integer>>),
    EFile1;
update_idx(EFile, _Pos, _Old, _New) ->
    EFile.

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
            lists:foldl(fun({RTime, REvent}, AccIn) ->
                                Fun(RTime, binary_to_term(REvent), AccIn)
                        end, Acc, RAcc)
    end;

read_recon(Start, End, EFile, Fun, Acc, <<>>, RAcc, Recon) ->
    file:close(Recon),
    read_recon(Start, End, EFile, Fun, Acc, <<>>, RAcc, undefined);


read_recon(Start, End, EFile, Fun, Acc,
           <<Size:64/integer, Time:128/integer, _:Size/binary, R/binary>>,
           RAcc, Recon)
  when Time < Start;
       Time > End ->
    read_recon(Start, End, EFile, Fun, Acc, R, RAcc, Recon);

read_recon(Start, End, EFile, Fun, Acc,
           <<Size:64/integer, Time:128/integer, Event:Size/binary, R/binary>>,
           RAcc, Recon) ->
    read_recon(Start, End, EFile, Fun, Acc, R, [{Time, Event} | RAcc], Recon);

read_recon(Start, End, EFile, Fun, Acc, Data, RAcc, Recon) ->
    case  file:read(Recon, EFile#efile.read_size) of
        {ok, Data1} ->
            read_recon(Start, End, EFile, Fun, Acc, <<Data/binary, Data1/binary>>,
                       RAcc, Recon);
        eof ->
            read_recon(Start, End, EFile, Fun, Acc, <<>>, RAcc, Recon)
    end.



read(Start, End, EFile = #efile{read_size = ReadSize, idx_t = Tree},
     Fun, Acc, undefined, RAcc, EStore) ->
    %% We find the first element that is at least
    %% as large as the start time.
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
            lists:foldl(fun({RTime, REvent}, AccIn) ->
                                Fun(RTime, binary_to_term(REvent), AccIn)
                        end, Acc, RAcc)
    end;

read(_Start, End, #efile{grace  = Grace}, Fun, Acc,
     <<_Size:64/integer, Time:128/integer, _/binary>>,
     RAcc, EStore)
  when Time > End + Grace ->
    file:close(EStore),
    %% TODO: read recon file
    lists:foldl(fun({RTime, REvent}, AccIn) ->
                        Fun(RTime, binary_to_term(REvent), AccIn)
                end, Acc, RAcc);

read(Start, End, EFile, Fun, Acc,
     <<Size:64/integer, Time:128/integer, _:Size/binary, R/binary>>,
     RAcc, EStore) when Time < Start ->
    read(Start, End, EFile, Fun, Acc, R, RAcc, EStore);

read(Start, End, EFile, Fun, Acc,
     <<_Size:64/integer, Time:128/integer, _/binary>> = Data,
     [{RTime, REvent} | RAcc], EStore) when
      Time >= RTime ->
    Acc1 = Fun(RTime, binary_to_term(REvent), Acc),
    read(Start, End, EFile, Fun, Acc1, Data, RAcc, EStore);

read(Start, End, EFile, Fun, Acc,
     <<Size:64/integer, Time:128/integer, Event:Size/binary, R/binary>>,
     RAcc, EStore) ->
    Acc1 = Fun(Time, binary_to_term(Event), Acc),
    read(Start, End, EFile, Fun, Acc1, R, RAcc, EStore);

read(Start, End, EFile, Fun, Acc, Data, RAcc, EStore) ->
    case  file:read(EStore, EFile#efile.read_size) of
        {ok, Data1} ->
            read(Start, End, EFile, Fun, Acc, <<Data/binary, Data1/binary>>,
                 RAcc, EStore);
        eof ->
            file:close(EStore),
            Acc
    end.

serialize(Es) ->
    << <<(serialize(Time, Event))/binary>> || {Time, Event} <- Es >>.

serialize(Time, Event) ->
    B = term_to_binary(Event),
    S = byte_size(B),
    <<S:64/integer, Time:128/integer, B/binary>>.

apply_opts(EFile, []) ->
    EFile;
apply_opts(EFile, [{grace, Grace} | R]) ->
    apply_opts(EFile#efile{grace = Grace}, R);
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
