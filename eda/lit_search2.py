"""Round 2: verify remaining citations via Crossref API (polite, permissive)."""
import json
import time
import urllib.request
import urllib.parse

QUERIES = [
    "data mining approach predict forest fires meteorological data Cortez",
    "review machine learning applications wildfire science Jain",
    "forest fire detection deep learning real-time review",
    "convolutional LSTM precipitation nowcasting Shi",
    "next day wildfire spread machine learning remote sensing Huot",
    "forest fire detection techniques review Alkhatib",
    "predictive modeling wildfire new dataset machine learning Sayad",
    "Kafka distributed messaging system log processing Kreps",
    "forest fire IoT detection wireless sensor network review",
    "wildfire susceptibility mapping machine learning review",
    "gridded surface meteorological data ecological applications Abatzoglou",
    "FireCast leveraging deep learning wildfire spread Radke",
    "smoke detection deep learning video fire CNN",
    "concept drift data stream mining survey",
    "fire weather index machine learning prediction",
    "barmpoutis review early forest fire detection optical remote sensing",
    "stream processing frameworks performance evaluation survey",
    "forest fire prediction artificial intelligence meteorological",
]

BASE = "https://api.crossref.org/works"
results = json.load(open("eda/lit_raw.json")) if __import__("os").path.exists("eda/lit_raw.json") else {}

for q in QUERIES:
    url = (f"{BASE}?query.bibliographic={urllib.parse.quote(q)}"
           "&rows=5&select=title,published,container-title,is-referenced-by-count,author,DOI")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "lit-review/1.0 (mailto:student@example.edu)"})
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read().decode())
        items = []
        for it in data.get("message", {}).get("items", []):
            title = (it.get("title") or [""])[0]
            if not title:
                continue
            pub = it.get("published", {}).get("date-parts", [[None]])[0]
            items.append({
                "title": title,
                "year": pub[0] if pub else None,
                "venue": (it.get("container-title") or [""])[0],
                "citations": it.get("is-referenced-by-count"),
                "authors": [f"{a.get('given','')} {a.get('family','')}".strip()
                            for a in (it.get("author") or [])][:3],
                "doi": it.get("DOI"),
            })
        results[q] = items
        print(f"OK  {q[:55]}: {len(items)} hits")
    except Exception as e:
        print(f"ERR {q[:55]}: {e}")
    time.sleep(0.8)

with open("eda/lit_raw2.json", "w") as f:
    json.dump(results, f, indent=2)
print("saved eda/lit_raw2.json")
