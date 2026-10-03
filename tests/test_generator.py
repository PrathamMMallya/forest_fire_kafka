from streaming.generator import generate_features, FEATURE_NAMES


def test_generate_features_keys():
    d = generate_features()
    assert set(d.keys()) == set(FEATURE_NAMES)


def test_generate_features_types_and_latitude():
    d = generate_features()
    for v in d.values():
        assert isinstance(v, (int, float))

    # check a stable field has expected value when drift is False
    d2 = generate_features(drift=False)
    assert d2["latitude"] == 48.12
