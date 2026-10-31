"""Groq LLM client with tool-calling, retries, timeouts and trace logging.

Design rules enforced here (single chokepoint for every agent):
  * every network call has a timeout and a bounded retry policy
  * every prompt + tool call + tool result is appended to logs/agent_traces.jsonl
  * the model never executes anything itself — it *requests* a tool, the
    Toolbelt (allowlisted) executes it and the result is fed back
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from pathlib import Path

import requests

log = logging.getLogger("agents.llm")

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-120b")
TRACE_FILE = Path(os.environ.get("AGENT_TRACE_FILE", "logs/agent_traces.jsonl"))

TIMEOUT_S = 30
MAX_RETRIES = 4
# 429s on Groq are per-minute token windows — back off long enough to clear one
BACKOFF_S = [8, 20, 40, 60]


class LLMError(RuntimeError):
    pass


class NonRetryableLLMError(LLMError):
    """Deterministic failure (e.g. context overflow) — retrying can't help."""


class GroqClient:
    """Minimal OpenAI-compatible chat client for Groq with tool calling."""

    def __init__(self, model: str = DEFAULT_MODEL, api_key: str | None = None):
        self.model = model
        self.api_key = api_key or os.environ.get("GROQ_API_KEY", "")
        if not self.api_key:
            raise LLMError("GROQ_API_KEY is not set — export it before running agents.")
        self.trace_id = uuid.uuid4().hex[:8]

    # ------------------------------------------------------------------ trace
    def _trace(self, kind: str, payload: dict) -> None:
        try:
            TRACE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with TRACE_FILE.open("a") as f:
                f.write(json.dumps({
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "trace": self.trace_id, "kind": kind, **payload,
                }) + "\n")
        except OSError:
            pass  # tracing must never break the agent loop

    # ------------------------------------------------------------------ call
    def chat(self, messages: list[dict], tools: list[dict] | None = None,
             temperature: float = 0.2, response_json: bool = False) -> dict:
        """One completion round. Returns the raw `choices[0].message` dict."""
        body: dict = {"model": self.model, "messages": messages,
                      "temperature": temperature}
        if tools:
            body["tools"] = tools
            body["tool_choice"] = "auto"
        if response_json:
            body["response_format"] = {"type": "json_object"}

        self._trace("request", {"messages": messages,
                                "tools": [t["function"]["name"] for t in tools or []]})
        last_err: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.post(
                    GROQ_URL, json=body, timeout=TIMEOUT_S,
                    headers={"Authorization": f"Bearer {self.api_key}",
                             "Content-Type": "application/json"})
                if r.status_code == 429 or r.status_code >= 500:
                    raise LLMError(f"groq {r.status_code}: {r.text[:200]}")
                if r.status_code == 413:  # payload too large — retrying can't help
                    raise NonRetryableLLMError(
                        f"groq 413 payload too large (context overflow): {r.text[:200]}")
                r.raise_for_status()
                msg = r.json()["choices"][0]["message"]
                self._trace("response", {"message": msg})
                return msg
            except (requests.RequestException, LLMError) as e:  # retryable
                last_err = e
                wait = BACKOFF_S[min(attempt - 1, len(BACKOFF_S) - 1)]
                log.warning("groq attempt %d/%d failed: %s (retry in %ds)",
                            attempt, MAX_RETRIES, e, wait)
                time.sleep(wait)
        raise LLMError(f"groq call failed after {MAX_RETRIES} attempts: {last_err}")
