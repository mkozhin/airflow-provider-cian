from airflow_provider_cian._version import __version__


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-cian",
        "name": "Cian",
        "description": "Airflow provider for Cian.ru Builder API",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "Cian",
                "external-doc-url": "https://public-api.cian.ru/builders/docs/latest",
                "tags": ["service"],
            },
        ],
        "operators": [
            {
                "integration-name": "Cian",
                "python-modules": ["airflow_provider_cian.operators.builder_reports"],
            },
        ],
        "hooks": [
            {
                "integration-name": "Cian",
                "python-modules": ["airflow_provider_cian.hooks.cian"],
            },
        ],
    }
