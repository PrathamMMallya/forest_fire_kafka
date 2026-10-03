import random

FEATURE_NAMES = [
    "latitude", "longitude", "tmmx", "tmmn", "pr", "srad", "vs", "pet",
    "fm100", "fm1000", "erc", "bi", "vpd", "rmax", "rmin", "sph", "etr"
]

# 8–9 features that will drift (weather + fuel)
DRIFT_COLUMNS = {
    "tmmx", "tmmn", "pr", "vpd",
    "fm100", "fm1000", "erc", "bi"
}

def generate_features(drift: bool = True) -> dict:
    """
    Generate one feature dict.
    drift=True -> induce drift only in DRIFT_COLUMNS
    """

    features = {}

    for name in FEATURE_NAMES:
        if drift and name in DRIFT_COLUMNS:
            # ❗ Intentionally out-of-distribution
            features[name] = random.uniform(0, 100)

        else:
            # ✅ Stable / realistic-ish values
            if name == "latitude":
                features[name] = 48.12
            elif name == "longitude":
                features[name] = -97.27
            elif name in {"tmmx"}:
                features[name] = random.uniform(295, 305)
            elif name in {"tmmn"}:
                features[name] = random.uniform(275, 290)
            elif name == "pr":
                features[name] = random.choice([0, 0, 0, random.uniform(0, 5)])
            elif name in {"fm100", "fm1000"}:
                features[name] = random.uniform(10, 20)
            elif name in {"erc", "bi"}:
                features[name] = random.uniform(30, 80)
            elif name in {"rmin", "rmax"}:
                features[name] = random.uniform(30, 70)
            elif name in {"srad", "pet", "etr"}:
                features[name] = random.uniform(5, 20)
            elif name in {"vs", "sph"}:
                features[name] = random.uniform(1, 10)
            else:
                features[name] = random.uniform(10, 40)

    return features
