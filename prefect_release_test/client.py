import httpx
from typing import Any

from prefect_release_test.settings import TestSettings


class TestClient:
    """Async HTTP client for Prefect API operations."""

    def __init__(self, settings: TestSettings):
        self.settings = settings
        self.base_url = settings.prefect_api_url.rstrip("/")
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "TestClient":
        headers = {"Content-Type": "application/json"}
        if self.settings.prefect_api_key:
            headers["Authorization"] = f"Bearer {self.settings.prefect_api_key}"
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=30.0,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Client not initialized. Use 'async with' context manager.")
        return self._client

    async def create_flow(self, name: str) -> dict[str, Any]:
        """Create a flow by name."""
        response = await self.client.post("/flows/", json={"name": name})
        response.raise_for_status()
        return response.json()

    async def create_deployment(
        self,
        name: str,
        flow_id: str,
        description: str | None = None,
        paused: bool = False,
        disabled: bool = False,
        parameters: dict[str, Any] | None = None,
        work_pool_name: str | None = None,
        work_queue_name: str | None = None,
        path: str | None = None,
        entrypoint: str | None = None,
        job_variables: dict[str, Any] | None = None,
        **kwargs,  # Accept and ignore legacy params like infrastructure_document_id
    ) -> dict[str, Any]:
        """Create a deployment."""
        payload = {
            "name": name,
            "flow_id": flow_id,
            "paused": paused,
        }
        if description:
            payload["description"] = description
        if parameters:
            payload["parameters"] = parameters
        if work_pool_name:
            payload["work_pool_name"] = work_pool_name
        if work_queue_name:
            payload["work_queue_name"] = work_queue_name
        if path:
            payload["path"] = path
        if entrypoint:
            payload["entrypoint"] = entrypoint
        if job_variables:
            payload["job_variables"] = job_variables

        response = await self.client.post("/deployments/", json=payload)
        response.raise_for_status()
        return response.json()

    async def run_deployment(self, deployment_id: str) -> dict[str, Any]:
        """Run a deployment and return the flow run."""
        response = await self.client.post(
            f"/deployments/{deployment_id}/create_flow_run",
            json={},
        )
        response.raise_for_status()
        return response.json()

    async def count_flowruns(self, filter_: dict[str, Any]) -> int:
        """Count flow runs matching the filter."""
        response = await self.client.post("/flow_runs/count", json=filter_)
        response.raise_for_status()
        return response.json()

    async def read_flowruns(self, filter_: dict[str, Any]) -> list[dict[str, Any]]:
        """Read flow runs matching the filter."""
        response = await self.client.post("/flow_runs/filter", json=filter_)
        response.raise_for_status()
        return response.json()

    async def set_flowrun_state(
        self, flowrun_id: str, state: dict[str, Any]
    ) -> dict[str, Any]:
        """Set the state of a flow run."""
        response = await self.client.post(
            f"/flow_runs/{flowrun_id}/set_state",
            json=state,
        )
        response.raise_for_status()
        return response.json()

    async def delete_flowrun(self, flowrun_id: str) -> None:
        """Delete a flow run."""
        response = await self.client.delete(f"/flow_runs/{flowrun_id}")
        response.raise_for_status()

    async def delete_deployment(self, deployment_id: str) -> None:
        """Delete a deployment."""
        response = await self.client.delete(f"/deployments/{deployment_id}")
        response.raise_for_status()

    async def delete_flow(self, flow_id: str) -> None:
        """Delete a flow."""
        response = await self.client.delete(f"/flows/{flow_id}")
        response.raise_for_status()
