"""
Airflow REST API client.

Thin httpx wrapper — all CLI commands go through this class.
Never imports from pipeline/ — this is the only HTTP layer in the CLI.

Authentication
--------------
Basic auth via AirflowSettings (AIRFLOW_USERNAME + AIRFLOW_PASSWORD).
Override the base URL via AIRFLOW_API_URL environment variable.

API version
-----------
Targets Airflow 2.x stable REST API (/api/v1/).
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

import httpx

from pipeline.settings import AirflowSettings, get_settings
from pipeline.exceptions import OrchestratorError

logger = logging.getLogger(__name__)

# Airflow REST API base path
_API_PREFIX = "/api/v1"


class AirflowClient:
    """HTTP client for the Airflow REST API.

    Parameters
    ----------
    settings:
        AirflowSettings instance. Defaults to get_settings() subset.
        Pass an explicit instance in tests to avoid real HTTP calls.

    Examples
    --------
    ::

        client = AirflowClient()
        run = client.trigger_dag("bronze_dag")
        print(run["dag_run_id"])
    """

    def __init__(self, settings: AirflowSettings | None = None) -> None:
        if settings is None:
            s = get_settings()
            # AirflowSettings lives in pipeline/settings.py but is only
            # used here in cli/ — never in Service classes.
            self._settings = AirflowSettings()
        else:
            self._settings = settings

        self._client = httpx.Client(
            base_url=self._settings.api_url,
            auth=(
                self._settings.username,
                self._settings.password.get_secret_value(),
            ),
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )

    # ------------------------------------------------------------------
    # DAG runs
    # ------------------------------------------------------------------

    def trigger_dag(
        self,
        dag_id: str,
        logical_date: str | None = None,
        conf: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Trigger a new DAG run.

        Parameters
        ----------
        dag_id:
            The DAG to trigger.
        logical_date:
            ISO-8601 datetime string for the logical date override.
            If None, Airflow uses the current time.
        conf:
            Optional JSON config passed to the DAG run.

        Returns
        -------
        dict
            The created DagRun object from the Airflow API.

        Raises
        ------
        OrchestratorError
            If the API returns a non-2xx response.
        """
        body: dict[str, Any] = {}
        if logical_date:
            body["logical_date"] = logical_date
        if conf:
            body["conf"] = conf

        response = self._post(f"{_API_PREFIX}/dags/{dag_id}/dagRuns", body)
        return response

    def get_dag_run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        """Fetch a specific DAG run by run_id."""
        return self._get(f"{_API_PREFIX}/dags/{dag_id}/dagRuns/{run_id}")

    def list_dag_runs(
        self,
        dag_id: str,
        limit: int = 10,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        """List recent DAG runs for a given DAG.

        Parameters
        ----------
        dag_id:
            The DAG to query.
        limit:
            Maximum number of runs to return.
        state:
            Filter by state: "running", "success", "failed", or None for all.
        """
        params: dict[str, Any] = {"limit": limit, "order_by": "-start_date"}
        if state:
            params["state"] = state

        response = self._get(
            f"{_API_PREFIX}/dags/{dag_id}/dagRuns",
            params=params,
        )
        return response.get("dag_runs", [])

    def list_all_dag_runs(
        self,
        dag_ids: list[str],
        limit: int = 5,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return recent runs for multiple DAGs keyed by dag_id."""
        return {dag_id: self.list_dag_runs(dag_id, limit=limit) for dag_id in dag_ids}

    # ------------------------------------------------------------------
    # Task instances
    # ------------------------------------------------------------------

    def list_task_instances(
        self,
        dag_id: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """List all task instances for a DAG run."""
        response = self._get(
            f"{_API_PREFIX}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
        )
        return response.get("task_instances", [])

    def get_task_log(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int = 1,
    ) -> str:
        """Fetch log content for a specific task instance.

        Returns
        -------
        str
            Raw log content.
        """
        response = self._get(
            f"{_API_PREFIX}/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}",
            headers={"Accept": "text/plain"},
        )
        # Log endpoint returns plain text, not JSON
        if isinstance(response, str):
            return response
        return str(response)

    def stream_task_log(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int = 1,
    ) -> Iterator[str]:
        """Stream task log lines to stdout."""
        url = (
            f"{self._settings.api_url}{_API_PREFIX}/dags/{dag_id}"
            f"/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
        )
        with httpx.stream(
            "GET",
            url,
            auth=(
                self._settings.username,
                self._settings.password.get_secret_value(),
            ),
            headers={"Accept": "text/plain"},
            timeout=60.0,
        ) as response:
            if response.status_code != 200:
                raise OrchestratorError(
                    f"stream logs {dag_id}/{task_id}",
                    status_code=response.status_code,
                )
            for line in response.iter_lines():
                yield line

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """Check Airflow API health."""
        return self._get(f"{_API_PREFIX}/health")

    # ------------------------------------------------------------------
    # Private HTTP helpers
    # ------------------------------------------------------------------

    def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        try:
            response = self._client.get(path, params=params, headers=headers)
            self._raise_for_status(response)
            content_type = response.headers.get("content-type", "")
            if "json" in content_type:
                return response.json()
            return response.text
        except httpx.RequestError as exc:
            raise OrchestratorError(
                f"GET {path}", cause=exc
            ) from exc

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self._client.post(path, json=body)
            self._raise_for_status(response)
            return response.json()  # type: ignore[return-value]
        except httpx.RequestError as exc:
            raise OrchestratorError(
                f"POST {path}", cause=exc
            ) from exc

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.status_code >= 400:
            raise OrchestratorError(
                f"{response.request.method} {response.request.url}",
                status_code=response.status_code,
            )

    def __enter__(self) -> AirflowClient:
        return self

    def __exit__(self, *args: object) -> None:
        self._client.close()

    def close(self) -> None:
        """Close the underlying httpx client."""
        self._client.close()
