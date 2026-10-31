"""Agentic AI layer for the Wildfire real-time detection system.

Multi-agent architecture on top of the existing Kafka/ES/FastAPI backbone:

    User / Alert stream
          │
          ▼
    CoordinatorAgent  (planner + delegator: decomposes goals, routes subtasks)
          │
          ├── AlertVerifierAgent   (checks alert evidence in Elasticsearch)
          ├── FireHistoryAgent     (has this location burned before?)
          ├── SituationAgent       (current conditions & pipeline health)
          └── DispatcherAgent      (drafts & sends emergency notifications)
          │
    CriticAgent (verifies every claim is grounded in tool evidence)
          │
    ChatAgent (user-facing Q&A over the whole system)

All agents share one LLM backend (Groq) and one toolbelt; tools are
allowlisted functions over the running system — never free-form execution.
"""
