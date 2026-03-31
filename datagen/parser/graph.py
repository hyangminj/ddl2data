from __future__ import annotations

from collections import defaultdict, deque

from datagen.config import TableMeta


def build_fk_graph(tables: list[TableMeta]) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {t.name: set() for t in tables}
    for table in tables:
        for fk in table.foreign_keys:
            if fk.ref_table in graph:
                graph[table.name].add(fk.ref_table)
    return graph


def generation_order(tables: list[TableMeta]) -> list[str]:
    graph = build_fk_graph(tables)

    reverse: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = {name: 0 for name in graph}
    for table, deps in graph.items():
        indegree[table] = len(deps)
        for dep in deps:
            reverse[dep].add(table)

    queue = deque([t for t, deg in indegree.items() if deg == 0])
    ordered: list[str] = []

    while queue:
        node = queue.popleft()
        ordered.append(node)
        for child in reverse[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(ordered) != len(graph):
        # cycle fallback: append remaining deterministically
        remain = sorted([k for k in graph if k not in ordered])
        ordered.extend(remain)

    return ordered
