"""Read paths: current projection, edition history, group aggregates.

The current projection adopts, per (study, observation), the row with the
highest edition number.  Every read path filters out persons present in the
withdrawal barrier, so a registered withdrawal hides people immediately -
even before its staged erasure has finished (or been resumed).
"""
from __future__ import annotations

PROJECTION_SQL = """
SELECT e.observation, e.person, e.edition, e.display_name, e.measure, e.memo
FROM editions e
JOIN (
  SELECT study, observation, MAX(edition) AS me
  FROM editions GROUP BY study, observation
) t ON t.study = e.study AND t.observation = e.observation AND t.me = e.edition
WHERE e.study = ?
  AND NOT EXISTS (SELECT 1 FROM barrier b WHERE b.study = e.study AND b.person = e.person)
ORDER BY e.observation
"""

HISTORY_SQL = """
SELECT e.edition, e.person, e.display_name, e.measure, e.memo, e.batch_tag
FROM editions e
WHERE e.study = ? AND e.observation = ?
  AND NOT EXISTS (SELECT 1 FROM barrier b WHERE b.study = e.study AND b.person = e.person)
ORDER BY e.edition
"""


def projection_rows(conn, study: str):
    return conn.execute(PROJECTION_SQL, (study,)).fetchall()


def history_rows(conn, study: str, observation: str):
    return conn.execute(HISTORY_SQL, (study, observation)).fetchall()


def row_dict(r) -> dict:
    return {"observation": r["observation"], "person": r["person"],
            "edition": r["edition"], "display_name": r["display_name"],
            "measure": r["measure"], "memo": r["memo"]}


def aggregate(rows, groups: dict) -> list:
    """Group adopted-edition rows by group_code.  `rows` must come from a
    single read snapshot so details and totals share one read point."""
    agg = {}
    for r in rows:
        g = groups.get(r["person"], "UNGROUPED")
        a = agg.setdefault(g, {"persons": set(), "measure_total": 0})
        a["persons"].add(r["person"])
        a["measure_total"] += r["measure"]
    return [{"group_code": g, "persons": len(a["persons"]),
             "measure_total": a["measure_total"]} for g, a in sorted(agg.items())]
