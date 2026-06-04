from airflow_provider_cian import get_provider_info


def test_provider_info_keys():
    info = get_provider_info()
    for key in ("package-name", "name", "description", "versions", "integrations", "operators", "hooks"):
        assert key in info, f"Missing key: {key}"


def test_provider_info_values():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-cian"
    assert info["name"] == "Cian"
    assert isinstance(info["versions"], list)
    assert len(info["versions"]) > 0
    assert isinstance(info["integrations"], list)
    assert isinstance(info["operators"], list)
    assert isinstance(info["hooks"], list)
