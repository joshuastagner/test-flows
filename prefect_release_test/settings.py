import os
from pydantic_settings import BaseSettings


class TestSettings(BaseSettings):
    """Configuration settings for Prefect release tests."""

    # GitHub repository URL for flow source code
    github_repo: str = "https://github.com/joshuastagner/test-flows.git"

    # Kubernetes work pool name
    workpool_k8s: str = "kubernetes-dev-customer-managed"

    # # Docker image reference (without tag)
    # image_reference: str = "prefecthq/prefect"

    # # Docker image version/tag
    # image_version: str = "latest"

    # Prefect API URL
    prefect_api_url: str = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

    # Prefect API key (optional, for cloud)
    prefect_api_key: str | None = os.getenv("PREFECT_API_KEY")

    class Config:
        env_prefix = "PREFECT_TEST_"
        env_file = ".env"
