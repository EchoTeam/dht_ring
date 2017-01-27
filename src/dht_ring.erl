%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(dht_ring).

-behaviour(gen_server).

% Public API
-export([
    add/2,
    delete/2,
    get_config/1,
    lookup/2,
    lookup_index/2,
    node_shares/1,
    nodes/1,
    partitions/1,
    partitions_if_node_added/2,
    set_opaque/3,
    start_link/1,
    start_link/2,
    stop/1
]).

% gen_server callbacks
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-record(state, { ring, nodes }).

%% Public API

add(Ring, {_Node, _Opaque, _Weight} = Peer) ->
    gen_server:call(Ring, {add, [Peer]});

add(Ring, Nodes) ->
    gen_server:call(Ring, {add, Nodes}).

delete(Ring, Node) when not is_list(Node) ->
    gen_server:call(Ring, {delete, [Node]});

delete(Ring, Nodes) ->
    gen_server:call(Ring, {delete, Nodes}).

get_config(Ring) ->
    gen_server:call(Ring, {get_config}).

lookup(Ring, Key) ->
    lookup_index(Ring, index(Key)).

lookup_index(Ring, Index) ->
    gen_server:call(Ring, {lookup, Index}, 15000).

nodes(Ring) ->
    gen_server:call(Ring, {nodes}).

node_shares(Ring) ->
    Partitions = partitions(Ring),
    NodePartitions = fun(Node) ->
            lists:foldl(fun
                    ({RN, From, To}, Acc) when RN == Node -> Acc + (To - From);
                    (_, Acc) -> Acc
                end, 0, Partitions)
    end,
    lists:flatten([io_lib:format("\t~p weight ~p share ~.2f%~n",
                [Node, Weight, Share])
            || {Node, _, Weight} <- get_config(Ring),
            Share <- [100 * NodePartitions(Node) / 65536]]).

partitions(Ring) ->
    partitions_from_ring(gen_server:call(Ring, {ring})).

partitions_if_node_added(Ring, Node) ->
    Nodes = get_config(Ring),
    {ok, S} = init([Node | Nodes]),
    partitions_from_ring(S#state.ring).

set_opaque(Ring, Node, Opaque) ->
    gen_server:call(Ring, {set_opaque, {Node, Opaque}}).

start_link(Peers) ->
    gen_server:start_link(?MODULE, Peers, []).

start_link(ServerName, Peers) ->
    gen_server:start_link({local, ServerName}, ?MODULE, Peers, []).

stop(RingPid) ->
    gen_server:call(RingPid, {stop}).

%% gen_server callbacks

handle_call({add, Nodes}, _From, #state{ nodes = OldNodes } = State) ->
    case [ N || {N, _, _} <- Nodes, lists:keymember(N, 1, OldNodes) ] of
        [] ->
            {ok, NewState} = init(OldNodes ++ Nodes),
            {reply, ok, NewState};
        Overlaps ->
            {reply, {error, already_there, Overlaps}, State}
    end;

handle_call({delete, Nodes}, _From, #state{ nodes = OldNodes } = State) ->
    case [ N || N <- Nodes, not lists:keymember(N, 1, OldNodes) ] of
        [] ->
            {ok, NewState} = init(
                [Node || {N, _, _} = Node <- OldNodes, not lists:member(N, Nodes)]
            ),
            {reply, ok, NewState};
        NotThere -> {reply, {error, unknown_nodes, NotThere}, State}
    end;

handle_call({get_config}, _From, #state{ nodes = Nodes } = State) ->
    {reply, Nodes, State};

handle_call({ring}, _From, #state{ ring = Ring } = State) ->
    {reply, Ring, State};

handle_call({lookup, KeyIndex}, _From, #state{ ring = Ring } = State) ->
    true = (KeyIndex >= 0) andalso (KeyIndex < 65536),
    case bsearch(Ring, KeyIndex) of
        empty -> {reply, [], State};
        PartIdx ->
            {_Hash, NodeList} = array:get(PartIdx, Ring),
            {reply, NodeList, State}
    end;

handle_call({nodes}, _From, #state{ nodes = Nodes } = State) ->
    {reply, [{Name, Opaque} || {Name, Opaque, _} <- Nodes], State};

handle_call({set_opaque, {Name, Opaque}}, _From, State) ->
    NewNodes = lists:map(fun
            ({N, _OldOpaque, Weight}) when N == Name -> {N, Opaque, Weight};
            (V) -> V
        end, State#state.nodes),
    NewRing = array:from_list(lists:map(fun({Hash, Data}) ->
                    {Hash, lists:map(fun
                                ({N, _OldOpaque}) when N == Name -> {N, Opaque};
                                (V) -> V
                            end, Data)}
            end, array:to_list(State#state.ring))),
    NewState = State#state{
        ring = NewRing,
        nodes = NewNodes
    },
    {reply, ok, NewState};

handle_call({stop}, _From, State) ->
    {stop, normal, stopped, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Request, State) ->
    {noreply, State}.

init(Peers) ->
    RawRing = lists:keysort(1,
        [{H, {Node, Opaque}} || {Node, Opaque, Weight} <- Peers,
            N <- lists:seq(1, Weight),
            H <- [index([atom_to_list(Node), integer_to_list(N)])]
        ]
    ),
    Ring = array:from_list(assemble_ring([], lists:reverse(RawRing), [], length(Peers))),
    error_logger:info_msg("Created a ring with ~b points in it.", [array:sparse_size(Ring)]),
    {ok, #state{ ring = Ring, nodes = Peers }}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

assemble_ring(_, [], R, _) -> R;
assemble_ring(H,[{Hash, {NN, _} = N} |T],R,L) ->
    ITN = [N|[E || {N2,_} = E<-H, N2 /= NN]],
    LITN = length(ITN),
    TN = case LITN == L of
        true -> ITN;
        false ->
            {_, RN} = try lists:foldr(
                    fun(_, {L2, Acc}) when L2==L -> throw({L2, Acc});
                        ({_, {N2, _} = E}, {L2, Acc}) ->
                            case lists:keymember(N2, 1, Acc) of
                                true -> {L2, Acc};
                                false -> {L2+1, Acc++[E]}
                            end
                    end, {LITN, ITN}, T)
                catch throw:V -> V end,
            RN
    end,
    assemble_ring(ITN,T,[{Hash,TN}|R],L).

calc_partitions([{Idx, [{Node, _} | _]}], FirstIdx, Acc) ->
    [{Node, 0, FirstIdx}, {Node, Idx, 65536} | Acc];
calc_partitions([{Idx1, [{Node, _} | _]}, {Idx2, _} = E | T], FirstIdx, Acc) ->
    calc_partitions([E|T], FirstIdx, [{Node, Idx1, Idx2} | Acc]).

partitions_from_ring(Ring) ->
    ArrL = array:to_list(Ring),
    [{Idx, _} | _] = ArrL,
    calc_partitions(ArrL, Idx, []).

index(Key) ->
    <<A,B,_/bytes>> = erlang:md5(term_to_binary(Key)),
    A bsl 8 + B.

% We rely on the fact that the array is kept intact after creation, e.g. no
% undefined entries exist in the middle.
bsearch(Arr, K) ->
    Size =  array:sparse_size(Arr),
    if Size == 0 -> empty; true -> bsearch(Arr, Size, 0, Size - 1, K) end.

bsearch(Arr, Size, LIdx, RIdx, K) ->
    MIdx = LIdx + (RIdx - LIdx + 1) div 2,
    true = (MIdx >= LIdx) andalso (MIdx =< RIdx),
    case key_fits(Arr, Size, MIdx - 1, MIdx, K) of
        {yes, Idx} -> Idx;
        {no, lt} -> bsearch(Arr, Size, LIdx, MIdx, K);
        {no, gt} ->
            if
                MIdx == (Size - 1) -> Size - 1;
                true -> bsearch(Arr, Size, MIdx, RIdx, K)
            end
    end.

key_fits(_Arr, 1, -1, 0, _K) ->
    {yes, 0};

key_fits(Arr, Size, -1, 0, K) ->
    {Hash0, _} = array:get(0, Arr),
    {HashS, _} = array:get(Size - 1, Arr),
    true = K < HashS,
    if
        K < Hash0 -> {yes, Size - 1};
        true -> {no, gt}
    end;

key_fits(Arr, Size, L, R, K) ->
    {LHash, _} = array:get(L, Arr),
    {RHash, _} = array:get(R, Arr),
    if
        K < LHash -> if L == 0 -> {yes, Size - 1}; true -> {no, lt} end;
        (K >= LHash) andalso (K < RHash) -> {yes, L};
        K >= RHash -> {no, gt}
    end.
