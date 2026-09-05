"""Tests for the PageRank cases required by the assignment."""

from __future__ import annotations

import unittest
from pathlib import Path

from pagerank import load_graph, run_pagerank

DAMPING = 0.85
TEST_DIR = Path(__file__).resolve().parent


class PageRankTests(unittest.TestCase):
    def test_three_node_chain_matches_manual_first_iteration(self) -> None:
        """A hand-calculated iteration verifies damping and dangling mass."""
        graph = {
            "A": ("B",),
            "B": ("C",),
            "C": (),
        }

        ranks = run_pagerank(
            graph,
            damping=DAMPING,
            max_iter=1,
            epsilon=1e-30,
        )

        # Initially every node has rank 1/3. C is dangling, so every node
        # receives 1/9 of redistributed mass. B and C also receive 1/3.
        expected_base = (1.0 - DAMPING) / 3
        expected_dangling_share = (1.0 / 3) / 3
        expected_a = expected_base + DAMPING * expected_dangling_share
        expected_b_and_c = expected_base + DAMPING * (1.0 / 3 + expected_dangling_share)

        self.assertAlmostEqual(ranks["A"], expected_a)
        self.assertAlmostEqual(ranks["B"], expected_b_and_c)
        self.assertAlmostEqual(ranks["C"], expected_b_and_c)

    def test_cycle_has_equal_ranks_by_symmetry(self) -> None:
        """A three-node cycle must preserve the uniform rank distribution."""
        graph = {
            "A": ("B",),
            "B": ("C",),
            "C": ("A",),
        }

        ranks = run_pagerank(graph)

        for rank in ranks.values():
            self.assertAlmostEqual(rank, 1.0 / 3)

    def test_dangling_node_rank_is_redistributed(self) -> None:
        """A dangling node must not leak rank from the two-node graph."""
        graph = {
            "A": ("B",),
            "B": (),
        }

        ranks = run_pagerank(graph, epsilon=1e-12, max_iter=200)

        # Solving the stationary equations gives A=20/57 and B=37/57.
        self.assertAlmostEqual(ranks["A"], 20.0 / 57, places=9)
        self.assertAlmostEqual(ranks["B"], 37.0 / 57, places=9)
        self.assertAlmostEqual(sum(ranks.values()), 1.0, places=12)

    def test_rank_sum_is_one_after_every_iteration(self) -> None:
        """The rank-mass invariant is checked after each of 30 passes."""
        graph = load_graph(TEST_DIR / "web_graph_sample.txt")

        for iteration in range(1, 31):
            with self.subTest(iteration=iteration):
                ranks = run_pagerank(
                    graph,
                    max_iter=iteration,
                    epsilon=1e-30,
                )
                self.assertAlmostEqual(sum(ranks.values()), 1.0, places=12)


if __name__ == "__main__":
    unittest.main(verbosity=2)
