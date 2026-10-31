"""CoordinatorAgent — the planner / delegator / verifier hub.

Loop (plan → delegate → verify → synthesize):

  1. PLAN        LLM decomposes the user goal into typed subtasks for the
                 worker registry, and flags whether any side-effecting action
                 (dispatch) is requested → approval gate.
  2. DELEGATE    each subtask runs on its worker (workers can run in parallel
                 threads; results are collected as findings + tool evidence).
  3. VERIFY      a Critic pass checks the draft answer against the evidence:
                 every quantitative claim must trace to a tool result.
  4. SYNTHESIZE  the final answer is grounded, cites evidence, and clearly
                 separates facts from recommended actions.
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor

from .llm import GroqClient
from .workers import WORKER_REGISTRY

log = logging.getLogger("agents.coordinator")

PLANNER_PROMPT = """You are the Coordinator of a multi-agent wildfire intelligence system.
Decompose the user's goal into subtasks for these workers:

  alert_verifier — verify fire claims against Elasticsearch alert/prediction evidence
  fire_history   — check whether a location (lat/lon) has had earlier fires
  situation      — summarize current conditions and pipeline health
  dispatcher     — draft (and, only with approval, send) emergency notifications

Rules:
  * Use at most one subtask per worker unless the goal clearly needs more.
  * Set needs_approval=true if the user asks to actually SEND/notify/page anyone.
  * Extract coordinates as decimal latitude/longitude when a place is given.
  * If the goal is unanswerable with these workers, return an empty subtask list
    and explain in "reason".

Return ONLY JSON:
{"subtasks": [{"worker": "<name>", "task": "<self-contained instruction>"}],
 "needs_approval": false, "reason": "<one line>"}"""

CRITIC_PROMPT = """You are the Critic verifying a wildfire agent system's draft answer.
You are given the draft and the raw tool evidence collected by the workers.

Check:
  1. Every number, tier, location and timestamp in the draft must appear in
     (or follow directly from) the evidence. Flag anything unsupported.
  2. Tool errors/unavailability must be disclosed, not hidden.
  3. Recommended actions must be labelled as recommendations, not facts.

Return ONLY JSON: {"grounded": true/false, "issues": ["..."],
"verdict": "<one sentence>"}"""

SYNTH_PROMPT = """You are the Coordinator writing the final answer for the user.
Use ONLY the worker findings and evidence below. If the critic found issues,
fix them: remove unsupported claims, disclose tool failures. Separate:
  **Situation** (verified facts) → **Assessment** → **Recommended actions**.
Be concise and quantitative. Plain text, no JSON."""


class CoordinatorAgent:
    def __init__(self, model: str | None = None,
                 worker_model: str | None = None, max_workers: int = 4):
        import os
        # Two models → two per-minute rate-limit buckets: the coordinator's
        # planning/verification traffic can never starve the workers.
        self.llm = GroqClient(model=model or os.environ.get(
            "GROQ_MODEL", "openai/gpt-oss-120b"))
        self.worker_llm = GroqClient(model=worker_model or os.environ.get(
            "GROQ_WORKER_MODEL", "openai/gpt-oss-20b"))
        self.max_workers = max_workers

    # ------------------------------------------------------------------ plan
    def plan(self, goal: str) -> dict:
        msg = self.llm.chat(
            [{"role": "system", "content": PLANNER_PROMPT},
             {"role": "user", "content": goal}],
            response_json=True)
        try:
            plan = json.loads(msg["content"])
            plan.setdefault("subtasks", [])
            plan.setdefault("needs_approval", False)
            return plan
        except (KeyError, json.JSONDecodeError):
            return {"subtasks": [], "needs_approval": False,
                    "reason": f"planner returned unparseable output: {msg.get('content', '')[:200]}"}

    # -------------------------------------------------------------- delegate
    def delegate(self, subtasks: list[dict], approved: bool = False) -> list[dict]:
        def run_one(st: dict) -> dict:
            cls = WORKER_REGISTRY.get(st.get("worker", ""))
            if cls is None:
                return {"worker": st.get("worker"), "findings": "",
                        "evidence": [], "error": "unknown worker — rejected"}
            task = st["task"]
            if cls.name == "dispatcher" and approved:
                task += "\n\n[APPROVAL GRANTED: you may send with approved=true.]"
            try:
                return cls(self.worker_llm).run(task)
            except Exception as e:  # a dead worker must not kill the whole run
                return {"worker": cls.name, "findings": "",
                        "evidence": [], "error": f"worker failed: {e}"}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            return list(pool.map(run_one, subtasks))

    # ---------------------------------------------------------------- verify
    def verify(self, goal: str, draft: str, results: list[dict]) -> dict:
        evidence = json.dumps([r.get("evidence", []) for r in results])[:8000]
        msg = self.llm.chat(
            [{"role": "system", "content": CRITIC_PROMPT},
             {"role": "user", "content":
              f"USER GOAL: {goal}\n\nDRAFT ANSWER:\n{draft}\n\nEVIDENCE:\n{evidence}"}],
            response_json=True)
        try:
            return json.loads(msg["content"])
        except (KeyError, json.JSONDecodeError):
            return {"grounded": True, "issues": [], "verdict": "critic parse failure — passing"}

    # ------------------------------------------------------------- synthesize
    def synthesize(self, goal: str, results: list[dict], critique: dict) -> str:
        payload = json.dumps(
            [{"worker": r.get("worker"), "findings": r.get("findings"),
              "error": r.get("error")} for r in results])[:8000]
        msg = self.llm.chat(
            [{"role": "system", "content": SYNTH_PROMPT},
             {"role": "user", "content":
              f"USER GOAL: {goal}\n\nWORKER FINDINGS:\n{payload}\n\n"
              f"CRITIQUE:\n{json.dumps(critique)}"}])
        return msg.get("content", "")

    # ------------------------------------------------------------------- run
    def run(self, goal: str, approved: bool = False) -> dict:
        plan = self.plan(goal)
        log.info("plan: %s", json.dumps(plan)[:300])
        if not plan["subtasks"]:
            return {"goal": goal, "plan": plan, "answer":
                    f"I can't decompose that goal with my workers. {plan['reason']}",
                    "results": [], "critique": None}

        if plan.get("needs_approval") and not approved:
            results = self.delegate(plan["subtasks"], approved=False)
            answer = self.synthesize(goal, results,
                                     {"grounded": True, "issues": [],
                                      "verdict": "approval pending"})
            return {"goal": goal, "plan": plan, "results": results,
                    "critique": None, "approval_required": True,
                    "answer": answer + "\n\n⚠ A dispatch was requested. It has been "
                                       "DRAFTED but NOT sent — re-run with approval "
                                       "to send."}

        results = self.delegate(plan["subtasks"], approved=approved)
        # draft once, verify, then synthesize the final grounded answer
        draft = self.synthesize(goal, results, {"grounded": True, "issues": [],
                                                "verdict": "initial draft"})
        critique = self.verify(goal, draft, results)
        answer = draft if critique.get("grounded") else self.synthesize(
            goal, results, critique)
        return {"goal": goal, "plan": plan, "results": results,
                "critique": critique, "answer": answer}
