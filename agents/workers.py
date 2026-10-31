"""Worker agents: narrow specialists the coordinator delegates to.

Each worker = one system prompt + an allowlisted subset of tools + a bounded
tool-calling loop. Workers never talk to the user and never call each other —
only the coordinator orchestrates them (hub-and-spoke, not free-for-all).
"""
from __future__ import annotations

import json
import logging

from .llm import GroqClient
from .tools import TOOL_REGISTRY, TOOL_SCHEMAS

log = logging.getLogger("agents.workers")

MAX_TOOL_ROUNDS = 4
TOOL_RESULT_CHARS = 2000  # keep worker contexts small (worker model has 8k TPM)


def assess_recurrence(prior_alert_count: int, by_tier: dict,
                      had_prior_emergency: bool) -> dict:
    """Turn raw fire-history tool output into a recurrence-risk verdict.

    TODO(student): this is the FireHistoryAgent's core judgment rule — refine it.
    Design trade-offs to consider:
      * Recency vs. frequency: is 1 EMERGENCY last week worse than 10 WATCHes
        five years ago? (the baseline ignores recency entirely)
      * Escalating severity: should a location whose tier history trends
        WATCH → WARNING → EMERGENCY score higher than a stable one?
      * False-positive cost: a "high" verdict pages humans — how conservative
        should the thresholds be given the pipeline is already recall-first?
    Baseline below: severity-weighted count with an emergency override.
    """
    weights = {"WATCH": 1, "WARNING": 3, "EMERGENCY": 6}
    score = sum(weights.get(t, 1) * n for t, n in by_tier.items())
    if had_prior_emergency or score >= 12:
        level = "high"
    elif score >= 4:
        level = "moderate"
    else:
        level = "low"
    return {"recurrence_score": score, "recurrence_level": level,
            "basis": f"{prior_alert_count} prior alerts, by tier: {by_tier}"}


class WorkerAgent:
    """Base worker: LLM + system prompt + allowlisted tools + loop."""

    name = "worker"
    system_prompt = ""
    tool_names: list[str] = []

    def __init__(self, llm: GroqClient):
        self.llm = llm
        self.schemas = [s for s in TOOL_SCHEMAS
                        if s["function"]["name"] in self.tool_names]

    def run(self, task: str) -> dict:
        """Execute one delegated subtask. Returns findings + raw evidence."""
        messages = [{"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": task}]
        evidence: list[dict] = []
        for _ in range(MAX_TOOL_ROUNDS):
            msg = self.llm.chat(messages, tools=self.schemas or None)
            messages.append(msg)
            calls = msg.get("tool_calls") or []
            if not calls:
                return {"worker": self.name,
                        "findings": msg.get("content", ""),
                        "evidence": evidence}
            for call in calls:
                result = self._execute(call)
                evidence.append({"tool": call["function"]["name"],
                                 "args": call["function"].get("arguments"),
                                 "result": result})
                messages.append({"role": "tool", "tool_call_id": call["id"],
                                 "content": json.dumps(result)[:TOOL_RESULT_CHARS]})
        return {"worker": self.name, "findings": "tool-round limit reached",
                "evidence": evidence}

    def _execute(self, call: dict) -> dict:
        """Allowlisted dispatch — the model picks a name, never code."""
        name = call["function"]["name"]
        fn = TOOL_REGISTRY.get(name)
        if fn is None or name not in self.tool_names:
            return {"error": f"tool '{name}' is not allowed for {self.name}"}
        try:
            args = json.loads(call["function"].get("arguments") or "{}")
            result = fn(**args)
            if name == "fire_history" and not result.get("unavailable"):
                result["assessment"] = assess_recurrence(
                    result.get("prior_alert_count", 0),
                    result.get("by_tier", {}),
                    result.get("had_prior_emergency", False))
            return result
        except TypeError as e:
            return {"error": f"bad arguments for {name}: {e}"}


class AlertVerifierAgent(WorkerAgent):
    """Checks whether live alerts are backed by Elasticsearch evidence."""
    name = "alert_verifier"
    tool_names = ["search_alerts", "recent_alerts", "latest_predictions"]
    system_prompt = (
        "You are the Alert Verifier for a wildfire detection system. Given a "
        "claim about fire activity, verify it against Elasticsearch evidence "
        "using your tools. Cross-check alerts against model predictions. Report "
        "what is confirmed, what is contradicted, and what is unverifiable. "
        "Cite concrete numbers (tier counts, probabilities, timestamps). "
        "Never invent alert data — if the tools are unavailable, say so.")


class FireHistoryAgent(WorkerAgent):
    """Answers: has this location burned before?"""
    name = "fire_history"
    tool_names = ["fire_history", "search_alerts"]
    system_prompt = (
        "You are the Fire History analyst. For any location you are given, "
        "use the fire_history geo tool to check for prior fire alerts near "
        "those coordinates. Interpret the recurrence assessment (score and "
        "level) in plain language for emergency planners: how often has this "
        "area burned, how severe, and what that implies. Always state the "
        "search radius you used.")


class SituationAgent(WorkerAgent):
    """Summarizes current conditions and pipeline health."""
    name = "situation"
    tool_names = ["pipeline_stats", "latest_predictions", "recent_alerts"]
    system_prompt = (
        "You are the Situation analyst for a live wildfire pipeline. Build a "
        "current picture: how many alerts by tier, where the highest-probability "
        "cells are, whether the pipeline itself is healthy (latency, "
        "throughput). Distinguish the *fire* situation from *system* health. "
        "Be quantitative.")


class DispatcherAgent(WorkerAgent):
    """Drafts emergency notifications; sending is gated behind approval."""
    name = "dispatcher"
    tool_names = ["draft_dispatch", "send_dispatch"]
    system_prompt = (
        "You are the Dispatcher. You draft clear, actionable emergency "
        "notifications from verified findings. You may ALWAYS draft. You may "
        "only call send_dispatch with approved=true when the task explicitly "
        "says approval was granted — otherwise send nothing and return the "
        "draft. Messages must include tier, region, key evidence, and one "
        "recommended action. No exaggeration, no minimization.")


WORKER_REGISTRY = {cls.name: cls for cls in [
    AlertVerifierAgent, FireHistoryAgent, SituationAgent, DispatcherAgent]}
