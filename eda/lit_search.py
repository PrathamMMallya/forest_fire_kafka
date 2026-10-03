"""Verify literature citations via Semantic Scholar API (real papers only)."""
import json
import time
import urllib.request
import urllib.parse

QUERIES = [
    "data mining approach predict forest fires meteorological data",
    "review machine learning applications wildfire science",
    "forest fire detection deep learning real-time",
    "convolutional LSTM precipitation nowcasting",
    "next day wildfire spread machine learning dataset",
    "wildfire spread prediction deep learning",
    "forest fire detection techniques review sensors",
    "predictive modeling wildfire new dataset machine learning",
    "Apache Kafka distributed messaging log processing",
    "Kafka Streams real-time stream processing",
    "forest fire IoT detection wireless sensor networks",
    "wildfire susceptibility mapping machine learning",
    "gridded surface meteorological data ecological applications",
    "FireCast deep learning wildfire spread prediction",
    "smoke detection video deep learning",
    "concept drift data stream mining survey",
    "fire danger rating system energy release component",
    "real-time forest fire detection streaming data",
]

BASE = "https://api.semanticscholar.org/graph/v1/paper/search"
FIELDS = "title,year,venue,citationCount,authors,externalIds"

results = {}
for q in QUERIES:
    url = f"{BASE}?query={urllib.parse.quote(q)}&limit=6&fields={FIELDS}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "academic-lit-review"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
        results[q] = [
            {
                "title": p.get("title"),
                "year": p.get("year"),
                "venue": p.get("venue"),
                "citations": p.get("citationCount"),
                "authors": [a["name"] for a in (p.get("authors") or [])][:3],
                "doi": (p.get("externalIds") or {}).get("DOI"),
            }
            for p in data.get("data", [])
        ]
        print(f"OK  {q}: {len(results[q])} hits")
    except Exception as e:
        print(f"ERR {q}: {e}")
        results[q] = []
    time.sleep(1.2)

with open("eda/lit_raw.json", "w") as f:
    json.dump(results, f, indent=2)
print("saved eda/lit_raw.json")
