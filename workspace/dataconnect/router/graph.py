"""Graph-based relationship walking for table discovery.

Builds a NetworkX graph from scanner relationships and walks it to find
tables connected to seed tables (from embedding matches). This expands
the candidate set by following foreign keys and discovered relationships.
"""

from __future__ import annotations

import logging

import networkx as nx

from dataconnect.config import RELATIONSHIP_DEPTH
from dataconnect.models import (
    MatchMethod,
    RelationshipInfo,
    TableMatch,
)

logger = logging.getLogger(__name__)


class RelationshipGraph:
    """Graph of table relationships for connected-table discovery.

    Nodes are table names. Edges represent relationships (FK, name match,
    value overlap, etc.) weighted by confidence.
    """

    def __init__(self) -> None:
        """Initialize an empty relationship graph."""
        self._graph: nx.Graph = nx.Graph()

    def build(self, relationships: list[RelationshipInfo]) -> None:
        """Build graph from scanner relationships.

        Args:
            relationships: All relationships from ScanResult.
        """
        self._graph.clear()

        for rel in relationships:
            src = rel.source_table
            tgt = rel.target_table

            if self._graph.has_edge(src, tgt):
                # Keep highest confidence edge
                existing = self._graph[src][tgt]["confidence"]
                if rel.confidence > existing:
                    self._graph[src][tgt]["confidence"] = rel.confidence
                    self._graph[src][tgt]["rel_type"] = rel.relationship_type.value
            else:
                self._graph.add_edge(
                    src, tgt,
                    confidence=rel.confidence,
                    rel_type=rel.relationship_type.value,
                )

        logger.info(
            "Built relationship graph: %d nodes, %d edges",
            self._graph.number_of_nodes(), self._graph.number_of_edges(),
        )

    def walk(
        self,
        seed_tables: list[str],
        max_depth: int = RELATIONSHIP_DEPTH,
    ) -> list[TableMatch]:
        """Walk the graph from seed tables to discover connected tables.

        Returns tables reachable within max_depth hops that are NOT
        already in the seed set. Relevance score decays with distance.

        Args:
            seed_tables: Starting tables (usually from embedding match).
            max_depth: Maximum hops from any seed table.

        Returns:
            List of newly discovered TableMatch results (excludes seeds).
        """
        if not seed_tables:
            return []

        discovered: dict[str, float] = {}  # table_name -> best score

        for seed in seed_tables:
            if seed not in self._graph:
                continue

            # BFS with depth tracking
            visited: set[str] = {seed}
            frontier: list[tuple[str, int]] = [(seed, 0)]

            while frontier:
                current, depth = frontier.pop(0)
                if depth >= max_depth:
                    continue

                for neighbor in self._graph.neighbors(current):
                    if neighbor in visited:
                        continue
                    visited.add(neighbor)

                    edge_confidence = self._graph[current][neighbor]["confidence"]
                    # Score decays with depth: confidence / (depth + 1)
                    hop_depth = depth + 1
                    score = edge_confidence / hop_depth

                    if neighbor not in seed_tables:
                        if neighbor not in discovered or score > discovered[neighbor]:
                            discovered[neighbor] = score

                    frontier.append((neighbor, hop_depth))

        # Sort by score descending
        matches: list[TableMatch] = []
        for table_name, score in sorted(
            discovered.items(), key=lambda x: -x[1],
        ):
            clamped = min(1.0, max(0.0, score))
            matches.append(TableMatch(
                table_name=table_name,
                methods=[MatchMethod.GRAPH_WALK],
                relevance_score=clamped,
                reasoning=f"Connected via relationship graph (score: {clamped:.3f})",
            ))

        return matches

    @property
    def node_count(self) -> int:
        """Number of tables in the graph."""
        return self._graph.number_of_nodes()

    @property
    def edge_count(self) -> int:
        """Number of relationships in the graph."""
        return self._graph.number_of_edges()
