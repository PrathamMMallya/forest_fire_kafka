"""Scripted demo of the multi-agent workflow (for the course presentation).

Runs three canonical scenarios end-to-end and prints the plan → delegation →
verification → answer trace for each. Requires the streaming stack
(Kafka/ES/api on :9991) to be up for live evidence; without it, tools return
'unavailable' and the agents demonstrate graceful degradation.

    export GROQ_API_KEY=...
    python -m agents.demo
"""
from __future__ import annotations

import json

from .coordinator import CoordinatorAgent

SCENARIOS = [
    ("Situational awareness",
     "Are there any active WARNING or EMERGENCY fire alerts right now? "
     "Summarize the current situation."),
    ("Location history lookup",
     "Has the area around latitude 47.59, longitude -119.57 had any earlier "
     "fires? What does that history imply for today?"),
    ("Action with approval gate",
     "Draft an emergency notification for the most severe current alert, "
     "but do not send it yet."),
]


def main() -> None:
    coord = CoordinatorAgent()
    for title, goal in SCENARIOS:
        print("=" * 78)
        print(f"SCENARIO: {title}\nGOAL: {goal}\n")
        out = coord.run(goal)
        print("PLAN:", json.dumps(out.get("plan"), indent=2)[:600], "\n")
        for r in out.get("results", []):
            print(f"  [{r.get('worker')}] {str(r.get('findings'))[:400]}")
        if out.get("critique"):
            print("\nCRITIQUE:", json.dumps(out["critique"])[:400])
        print("\nFINAL ANSWER:\n" + out["answer"])
        print()


if __name__ == "__main__":
    main()
