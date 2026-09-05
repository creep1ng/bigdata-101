"""PageRank implemented as repeated passes over the course MapReduce framework."""

from __future__ import annotations

import argparse
import math
import sys
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import TypeAlias

SCRIPT_DIR = Path(__file__).resolve().parent
FRAMEWORK_DIR = (
    SCRIPT_DIR.parents[1] / "modules" / "01-mapreduce" / "01-pure-python" / "01-basics"
)
sys.path.insert(0, str(FRAMEWORK_DIR))

from mapreduce_framework import mapreduce  # noqa: E402

Graph: TypeAlias = dict[str, tuple[str, ...]]
NodeState: TypeAlias = tuple[str, float, tuple[str, ...]]
Message: TypeAlias = tuple[str, object]

STRUCT = "STRUCT"
RANK = "RANK"


def load_graph(path: str | Path) -> Graph:
    """Read ``node: neighbor ...`` lines and include implicit neighbor nodes."""
    graph: Graph = {}
    input_path = Path(path)

    with input_path.open(encoding="utf-8") as graph_file:
        for line_number, raw_line in enumerate(graph_file, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                raise ValueError(
                    f"Invalid graph line {line_number}: expected 'node: neighbors'"
                )

            node, raw_neighbors = line.split(":", maxsplit=1)
            node = node.strip()
            if not node:
                raise ValueError(f"Invalid graph line {line_number}: empty node name")
            if node in graph:
                raise ValueError(
                    f"Invalid graph line {line_number}: duplicate node {node!r}"
                )

            graph[node] = tuple(raw_neighbors.split())

    referenced_nodes = {
        neighbor for neighbors in graph.values() for neighbor in neighbors
    }
    for node in sorted(referenced_nodes - graph.keys()):
        graph[node] = ()

    if not graph:
        raise ValueError("The graph must contain at least one node")

    return graph


def mapper(item: NodeState) -> Iterator[tuple[str, Message]]:
    """Preserve adjacency and emit rank contributions keyed by destination."""
    node, rank, neighbors = item
    yield node, (STRUCT, neighbors)

    if neighbors:
        contribution = rank / len(neighbors)
        for neighbor in neighbors:
            yield neighbor, (RANK, contribution)


def make_reducer(node_count: int, damping: float, dangling_mass: float):
    """Create the two-argument reducer needed for one PageRank iteration."""
    base_rank = (1.0 - damping) / node_count
    dangling_share = dangling_mass / node_count

    def reducer(node: str, values: list[Message]) -> NodeState:
        neighbors: tuple[str, ...] | None = None
        incoming_rank = 0.0

        for message_type, payload in values:
            if message_type == STRUCT:
                if neighbors is not None:
                    raise ValueError(f"Node {node!r} received more than one STRUCT")
                neighbors = tuple(payload)  # type: ignore[arg-type]
            elif message_type == RANK:
                incoming_rank += float(payload)
            else:
                raise ValueError(f"Unknown message type {message_type!r}")

        if neighbors is None:
            raise ValueError(f"Node {node!r} did not receive its STRUCT message")

        new_rank = base_rank + damping * (incoming_rank + dangling_share)
        return node, new_rank, neighbors

    return reducer


def _initial_state(graph: Graph) -> list[NodeState]:
    initial_rank = 1.0 / len(graph)
    return [(node, initial_rank, neighbors) for node, neighbors in graph.items()]


def _run_pagerank(
    graph: Graph,
    damping: float,
    max_iter: int,
    epsilon: float,
) -> tuple[dict[str, float], list[float], bool]:
    if not 0.0 <= damping < 1.0:
        raise ValueError("damping must satisfy 0 <= damping < 1")
    if max_iter < 1:
        raise ValueError("max_iter must be at least 1")
    if epsilon <= 0.0:
        raise ValueError("epsilon must be greater than 0")

    state = _initial_state(graph)
    node_count = len(state)
    l1_history: list[float] = []

    for _ in range(max_iter):
        old_ranks = {node: rank for node, rank, _ in state}
        dangling_mass = sum(rank for _, rank, neighbors in state if not neighbors)

        reduced = mapreduce(
            state,
            mapper,
            make_reducer(node_count, damping, dangling_mass),
        )
        state = list(reduced.values())

        l1_error = sum(abs(rank - old_ranks[node]) for node, rank, _ in state)
        l1_history.append(l1_error)
        if l1_error < epsilon:
            break

    ranks = {node: rank for node, rank, _ in state}
    return ranks, l1_history, l1_history[-1] < epsilon


def run_pagerank(
    graph: Graph,
    damping: float = 0.85,
    max_iter: int = 50,
    epsilon: float = 1e-6,
) -> dict[str, float]:
    """Calculate PageRank until L1 convergence or the iteration limit."""
    ranks, _, _ = _run_pagerank(graph, damping, max_iter, epsilon)
    return ranks


def in_degrees(graph: Graph) -> dict[str, int]:
    """Count incoming edges for every node."""
    degrees = dict.fromkeys(graph, 0)
    for neighbors in graph.values():
        for neighbor in neighbors:
            degrees[neighbor] += 1
    return degrees


def pearson_correlation(xs: Sequence[float], ys: Sequence[float]) -> float:
    """Return Pearson's r, or NaN when either input has zero variance."""
    if len(xs) != len(ys) or not xs:
        raise ValueError("Both inputs must have the same non-zero length")

    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    centered = [(x - mean_x, y - mean_y) for x, y in zip(xs, ys)]
    numerator = sum(x * y for x, y in centered)
    denominator = math.sqrt(
        sum(x * x for x, _ in centered) * sum(y * y for _, y in centered)
    )
    return numerator / denominator if denominator else math.nan


def print_analysis(
    graph: Graph,
    ranks: dict[str, float],
    l1_history: Sequence[float],
    converged: bool,
    elapsed_seconds: float,
    top_count: int,
) -> None:
    """Print the execution evidence and top-rank/in-degree comparison."""
    node_count = len(graph)
    edge_count = sum(len(neighbors) for neighbors in graph.values())
    dangling_count = sum(not neighbors for neighbors in graph.values())
    iterations = len(l1_history)
    pairs_per_iteration = node_count + edge_count
    degrees = in_degrees(graph)
    busiest_node, maximum_in_degree = max(
        degrees.items(), key=lambda item: (item[1], item[0])
    )
    correlation = pearson_correlation(
        list(ranks.values()), [float(degrees[node]) for node in ranks]
    )

    print("PageRank execution analysis")
    print("=" * 72)
    print(f"Nodes (N):                 {node_count:,}")
    print(f"Edges (E):                 {edge_count:,}")
    print(f"Dangling nodes:            {dangling_count:,}")
    print(f"Iterations:                {iterations:,}")
    print(f"Converged:                 {'yes' if converged else 'no'}")
    print(f"Final L1 difference:       {l1_history[-1]:.3e}")
    print(f"Rank sum invariant:        {sum(ranks.values()):.12f}")
    print(f"Mapper pairs / iteration:  {pairs_per_iteration:,} (N + E)")
    print(f"Total shuffled pairs:      {pairs_per_iteration * iterations:,}")
    print(f"Full graph passes:         {iterations:,}")
    print(
        f"Largest reduce group:      {busiest_node} "
        f"({maximum_in_degree + 1:,} messages)"
    )
    print(f"Elapsed time:              {elapsed_seconds:.3f} s")
    print(f"PageRank/in-degree r:      {correlation:.4f}")

    print(f"\nTop {min(top_count, node_count)} pages")
    print("-" * 72)
    print(f"{'Position':>8}  {'Page':<16} {'PageRank':>14} {'In-degree':>12}")
    print("-" * 72)
    ranking = sorted(ranks.items(), key=lambda item: (-item[1], item[0]))
    for position, (node, rank) in enumerate(ranking[:top_count], start=1):
        print(f"{position:>8}  {node:<16} {rank:>14.10f} {degrees[node]:>12}")

    print(
        "\nThe correlation is not perfect because PageRank weights each incoming "
        "link by the source page's rank and divides it by that source's out-degree."
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run iterative PageRank with the course MapReduce framework."
    )
    parser.add_argument(
        "graph",
        nargs="?",
        type=Path,
        default=SCRIPT_DIR / "web_graph_large.txt",
        help="graph file (default: web_graph_large.txt next to this script)",
    )
    parser.add_argument("--damping", type=float, default=0.85)
    parser.add_argument("--max-iter", type=int, default=50)
    parser.add_argument("--epsilon", type=float, default=1e-6)
    parser.add_argument("--top", type=int, default=15)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.top < 1:
        raise ValueError("top must be at least 1")

    graph = load_graph(args.graph)
    started_at = time.perf_counter()
    ranks, l1_history, converged = _run_pagerank(
        graph,
        damping=args.damping,
        max_iter=args.max_iter,
        epsilon=args.epsilon,
    )
    elapsed_seconds = time.perf_counter() - started_at

    print(f"Input graph: {args.graph.resolve()}")
    print_analysis(
        graph,
        ranks,
        l1_history,
        converged,
        elapsed_seconds,
        args.top,
    )
    return 0 if converged else 1


if __name__ == "__main__":
    raise SystemExit(main())
