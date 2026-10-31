"""User-facing Q&A agent — the front door to the multi-agent system.

Usage:
    export GROQ_API_KEY=...
    python -m agents.chat                                  # interactive REPL
    python -m agents.chat "Has 47.6, -119.6 burned before?" # one-shot
    python -m agents.chat --approve "Evacuation notice for the Boise cell"  # gated send

The chat agent keeps conversational context and hands every user turn to the
Coordinator, which plans, delegates, verifies and answers. Side-effecting
requests (send/page/notify) come back with an approval prompt unless
--approve was passed.
"""
from __future__ import annotations

import argparse
import json
import sys

from .coordinator import CoordinatorAgent

BANNER = """🔥 Wildfire Agentic Intelligence — ask anything about the live system.
   e.g. "any active emergencies right now?"
        "has the area around 47.6, -119.6 had fires before?"
        "draft an alert for the worst current cell"
   Type 'quit' to exit."""


def ask(coord: CoordinatorAgent, query: str, approved: bool = False) -> str:
    out = coord.run(query, approved=approved)
    return out["answer"]


def main() -> None:
    ap = argparse.ArgumentParser(description="Wildfire agentic Q&A")
    ap.add_argument("query", nargs="?", help="one-shot question (omit for REPL)")
    ap.add_argument("--approve", action="store_true",
                    help="grant dispatch approval for this run")
    ap.add_argument("--show-trace", action="store_true",
                    help="print plan + critique alongside the answer")
    args = ap.parse_args()

    try:
        coord = CoordinatorAgent()
    except Exception as e:
        sys.exit(f"Agent init failed: {e}")

    if args.query:
        out = coord.run(args.query, approved=args.approve)
        print(out["answer"])
        if args.show_trace:
            print("\n--- plan ---\n" + json.dumps(out.get("plan"), indent=2))
            print("--- critique ---\n" + json.dumps(out.get("critique"), indent=2))
        return

    print(BANNER)
    while True:
        try:
            q = input("\nYou > ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if q.lower() in {"quit", "exit", ""}:
            break
        out = coord.run(q, approved=args.approve)
        print(f"\nAgent > {out['answer']}")
        if out.get("approval_required"):
            yn = input("Send the drafted dispatch? [y/N] ").strip().lower()
            if yn == "y":
                out2 = coord.run(q + " (approval granted — send it)", approved=True)
                print(f"\nAgent > {out2['answer']}")


if __name__ == "__main__":
    main()
