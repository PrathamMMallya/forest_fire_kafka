"""Toolbelt: the *only* actions agents can take on the wildfire system.

Every tool is an explicit, allowlisted function over a real endpoint of the
running pipeline (FastAPI dashboard :9991, Elasticsearch :9200, notifier
webhook). Agents can never run arbitrary code — the coordinator maps a tool
*name* to one of these functions, anything else is rejected.

Side-effecting tools (send_dispatch) are gated: they refuse to run unless the
coordinator passes approved=True after human/critic approval.
"""
from __future__ import annotations

import logging
import math
import os
import time

import requests

log = logging.getLogger("agents.tools")

API = os.environ.get("WILDFIRE_API", "http://localhost:9991")
ES = os.environ.get("WILDFIRE_ES", "http://localhost:9200")
ES_INDEX = os.environ.get("WILDFIRE_ES_INDEX", "wildfire-alerts")
WEBHOOK = os.environ.get("EMERGENCY_WEBHOOK_URL", "")
HTTP_TIMEOUT = 8


def _get(url: str, **params) -> dict:
    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:  # tools must degrade gracefully, not crash the agent
        return {"error": f"{type(e).__name__}: {e}", "unavailable": True}


# ---------------------------------------------------------------- read tools
def search_alerts(query: str = "", tier: str = "", size: int = 10) -> dict:
    """Full-text + tier search over the Elasticsearch wildfire-alerts index."""
    return _get(f"{API}/api/search", q=query, tier=tier, size=size)


def recent_alerts(limit: int = 20) -> dict:
    """Most recent alerts as seen by the live dashboard."""
    return _get(f"{API}/api/alerts", limit=limit)


def latest_predictions(limit: int = 20) -> dict:
    """Latest model predictions (probability, tier, cell, timestamp)."""
    return _get(f"{API}/api/predictions", limit=limit)


def pipeline_stats() -> dict:
    """Live pipeline health: throughput, latency percentiles, tier counts."""
    return _get(f"{API}/api/stats")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def fire_history(latitude: float, longitude: float,
                 radius_km: float = 25.0, size: int = 200) -> dict:
    """Has this location burned before? Past alerts near a point.

    The index stores location as {lat, lon} floats (not geo_point), so we use
    a lat/lon range "bounding box" query server-side, then refine to a true
    radius with haversine client-side. Returns count by tier, most recent
    events, and the closest prior alert.
    """
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * max(0.1, abs(math.cos(math.radians(latitude)))))
    body = {
        "size": size,
        "query": {"bool": {"filter": [
            {"range": {"location.lat": {"gte": latitude - dlat,
                                        "lte": latitude + dlat}}},
            {"range": {"location.lon": {"gte": longitude - dlon,
                                        "lte": longitude + dlon}}}]}},
        "sort": [{"ts": {"order": "desc"}}],
    }
    try:
        r = requests.get(f"{ES}/{ES_INDEX}/_search", json=body, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        hits = r.json().get("hits", {}).get("hits", [])
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "unavailable": True}

    by_tier: dict[str, int] = {}
    events = []
    closest: dict | None = None
    for h in hits:
        src = h.get("_source", {})
        loc = src.get("location") or {}
        dist = _haversine_km(latitude, longitude,
                             loc.get("lat", 0), loc.get("lon", 0))
        if dist > radius_km:
            continue  # bounding-box corner outside the true radius
        tier = src.get("tier", "UNKNOWN")
        by_tier[tier] = by_tier.get(tier, 0) + 1
        event = {"ts": src.get("event_ts"), "tier": tier,
                 "probability": src.get("probability"),
                 "region": src.get("region_name"),
                 "distance_km": round(dist, 1)}
        events.append(event)
        if closest is None or dist < closest["distance_km"]:
            closest = event
    return {"center": {"lat": latitude, "lon": longitude},
            "radius_km": radius_km,
            "prior_alert_count": len(events),
            "by_tier": by_tier,
            "most_recent": events[:10],
            "closest": closest,
            "had_prior_emergency": by_tier.get("EMERGENCY", 0) > 0}


# -------------------------------------------------------------- action tools
def draft_dispatch(region: str, tier: str, summary: str,
                   recommended_action: str) -> dict:
    """Draft an emergency notification. NEVER sends — returns the draft only."""
    return {"draft": True, "region": region, "tier": tier,
            "message": f"[{tier}] Wildfire risk in {region}: {summary} "
                       f"Recommended action: {recommended_action}",
            "created_ts": time.time()}


def send_dispatch(message: str, approved: bool = False) -> dict:
    """Send an emergency dispatch to the notifier webhook.

    GATED: refuses unless approved=True (coordinator must obtain critic /
    human approval first). This is the only side-effecting tool.
    """
    if not approved:
        return {"sent": False, "refused": True,
                "reason": "dispatch requires approved=True (human/critic gate)"}
    if not WEBHOOK:
        return {"sent": False, "refused": True,
                "reason": "EMERGENCY_WEBHOOK_URL not configured — console only"}
    try:
        r = requests.post(WEBHOOK, json={"message": message,
                                         "source": "agentic-layer"},
                          timeout=HTTP_TIMEOUT)
        return {"sent": r.ok, "status": r.status_code}
    except Exception as e:
        return {"sent": False, "error": str(e)}


# ------------------------------------------------------------------ registry
TOOL_REGISTRY = {fn.__name__: fn for fn in [
    search_alerts, recent_alerts, latest_predictions, pipeline_stats,
    fire_history, draft_dispatch, send_dispatch,
]}

# OpenAI/Groq tool schemas (names must match TOOL_REGISTRY exactly).
TOOL_SCHEMAS = [
    {"type": "function", "function": {
        "name": "search_alerts",
        "description": "Full-text and tier search over Elasticsearch wildfire alerts.",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"}, "tier": {"type": "string",
                       "enum": ["", "WATCH", "WARNING", "EMERGENCY"]},
            "size": {"type": "integer", "default": 10}}}}},
    {"type": "function", "function": {
        "name": "recent_alerts",
        "description": "Get the most recent fire alerts from the live pipeline.",
        "parameters": {"type": "object", "properties": {
            "limit": {"type": "integer", "default": 20}}}}},
    {"type": "function", "function": {
        "name": "latest_predictions",
        "description": "Get the latest ML fire predictions (probability, tier, grid cell).",
        "parameters": {"type": "object", "properties": {
            "limit": {"type": "integer", "default": 20}}}}},
    {"type": "function", "function": {
        "name": "pipeline_stats",
        "description": "Get live pipeline health: throughput, latency, tier counts.",
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "fire_history",
        "description": "Check whether a lat/lon location has had earlier fire alerts "
                       "(geo search over historical alerts within a radius).",
        "parameters": {"type": "object", "properties": {
            "latitude": {"type": "number"}, "longitude": {"type": "number"},
            "radius_km": {"type": "number", "default": 25.0},
            "size": {"type": "integer", "default": 50}},
            "required": ["latitude", "longitude"]}}},
    {"type": "function", "function": {
        "name": "draft_dispatch",
        "description": "Draft (never send) an emergency notification message.",
        "parameters": {"type": "object", "properties": {
            "region": {"type": "string"}, "tier": {"type": "string"},
            "summary": {"type": "string"},
            "recommended_action": {"type": "string"}},
            "required": ["region", "tier", "summary", "recommended_action"]}}},
    {"type": "function", "function": {
        "name": "send_dispatch",
        "description": "Send an emergency dispatch. GATED: only runs with approved=true "
                       "after human or critic approval.",
        "parameters": {"type": "object", "properties": {
            "message": {"type": "string"},
            "approved": {"type": "boolean", "default": False}},
            "required": ["message"]}}},
]
