-module(test_dht_ring).
-compile(export_all).

start() ->
  {ok, RingServer} = dht_ring:start_link([{undef, undef, 1000}]),
  lists:foreach(
    fun(X) -> dht_ring:lookup(RingServer, integer_to_list(X)) end,
    lists:seq(1, 10)
  ).

test(Nodes, KeysQty, Expected) ->
  {ok, RingServer} = dht_ring:start_link(Nodes),

  true = (length(Nodes) == length(dht_ring:nodes(RingServer))),
  true = KeysQty >= 1,

  Keys = lists:seq(1, KeysQty),

  Results = lists:map(
    fun(X) -> dht_ring:lookup(RingServer, integer_to_list(X)) end,
    Keys
  ),

  lists:foreach(
    fun({K, E, G}) ->
      if
        E /= G -> io:format("FAIL: key=~p expected=~p got=~p~n", [K, E, G]);
        true -> ok
      end
    end,
    lists:zip3(Keys, Expected, Results)
  ).

test_dynamic() ->
  A = {a, a, 5},
  B = {b, b, 3},
  C = {c, c, 10},
  {ok, Ring} = dht_ring:start_link([A]),
  {ok, Ring0} = dht_ring:start_link([]),
  {ok, RingA} = dht_ring:start_link([A]),
  {ok, RingAB} = dht_ring:start_link([A, B]),
  {ok, RingAC} = dht_ring:start_link([A, C]),
  {ok, RingABC} = dht_ring:start_link([A, B, C]),

  {error, already_there, [a]} = dht_ring:add(Ring, [A]),

  Keys = lists:seq(1, 10),

  true = (length(dht_ring:nodes(Ring)) == 1),
  ok = dht_ring:add(Ring, [B]),
  true = (length(dht_ring:nodes(Ring)) == 2),

  Tests = [
    { "A+B vs AB", compare_rings(Ring, RingAB, Keys) },

    { "A+B vs AB config", compare_configs(Ring, RingAB) },

    begin
      ok = dht_ring:add(Ring, [C]),
      true = (length(dht_ring:nodes(Ring)) == 3),
      { "A+B+C vs ABC", compare_rings(Ring, RingABC, Keys) }
    end,

    { "A+B+C vs ABC config", compare_configs(Ring, RingABC) },

    % Check if 'nodes/1' returns the right thing
    { "nodes in A+B+C", 
      begin
        Nodes = lists:keysort(1, dht_ring:nodes(Ring)),
        if
          Nodes == [{a, a}, {b, b}, {c, c}] -> pass;
          true -> {fail, Nodes}
        end
      end
    },

    begin
      {error, unknown_nodes, [d, e]} = dht_ring:delete(Ring, [d, e]),
      ok = dht_ring:delete(Ring, [b]),
      { "A+C vs AC", compare_rings(Ring, RingAC, Keys) }
    end,

    { "A+C vs AC config", compare_configs(Ring, RingAC) },

    begin
      ok = dht_ring:delete(Ring, [a, c]),
      {"empty rings lookups", compare_rings(Ring, Ring0, Keys) }
    end,

    begin
      ok = dht_ring:add(Ring, A),
      { "0,add{A} vs A", compare_configs(Ring, RingA) }
    end,

    begin
      ok = dht_ring:delete(Ring, a),
      { "A,delete{A} vs 0", compare_configs(Ring, Ring0) }
    end
  ],

  case [ Result || {_, Status} = Result <- Tests, Status =/= pass ] of
    [] -> pass;
    Else -> {fail, Else}
  end.

compare_rings(Ring1, Ring2, Keys) ->
  Results = [ {Key, dht_ring:lookup(Ring1, Key) == dht_ring:lookup(Ring2, Key)}
    || Key <- Keys
  ],

  case [ Key || {Key, false} <- Results ] of
    [] -> pass;
    FailedKeys -> {fail, FailedKeys}
  end.


compare_configs(Ring1, Ring2) ->
  Config1 = dht_ring:get_config(Ring1),
  Config2 = dht_ring:get_config(Ring2),

  case lists:keysort(1, Config1) == lists:keysort(1, Config2) of
    true -> pass;
    _ -> {fail, Config1, Config2}
  end.


test() ->
  test(
    [{a, a, 5}, {b, b, 3}, {c, c, 2}],
    10,
    [
      [{a,a},{b,b},{c,c}],
      [{a,a},{c,c},{b,b}],
      [{c,c},{a,a},{b,b}],
      [{a,a},{b,b},{c,c}],
      [{a,a},{b,b},{c,c}],
      [{c,c},{a,a},{b,b}],
      [{a,a},{b,b},{c,c}],
      [{c,c},{a,a},{b,b}],
      [{b,b},{a,a},{c,c}],
      [{b,b},{a,a},{c,c}]
    ]
  ),

  test(
    [{n, n, 1}],
    10,
    lists:duplicate(10, [{n, n}])
  ),

  test_dynamic().

profile() ->
  eprof:start(),
  eprof:profile([], ?MODULE, start, []),
  eprof:analyse().
