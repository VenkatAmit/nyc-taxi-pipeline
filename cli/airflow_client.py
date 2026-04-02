"""
Airflow REST API client.

Thin httpx wrapper — all CLI commands go through this class.
Never imports from pipeline/ Service classes.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any, cast

import httpx

from cli.config import AirflowSettings, get_airflow_settings
from cli.exceptions import OrchestratorError

logger = logging.getLogger(__name__)

_API_PREFIX = "/api/v1"


class AirflowClient:
    """HTTP client for the Airflow REST API."""

    def __init__(self, settings: AirflowSettings | None = None) -> None:
        self._settings: AirflowSettings = (
            settings if settings is not None else get_airflow_settings()
        )
        self._client = httpx.Client(
            base_url=self._settings.api_url,
            auth=(
                self._settings.username,
                self._settings.password.get_secret_value(),
            ),
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )

    def trigger_dag(
        self,
        dag_id: str,
        logical_date: str | None = None,
        conf: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Trigger a new DAG run."""
        body: dict[str, Any] = {}
        if logical_date:
            body["logical_date"] = logical_date
        if conf:
            body["conf"] = conf
        return self._post(f"{_API_PREFIX}/dags/{dag_id}/dagRuns", body)

    def get_dag_run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        return self._get_json(f"{_API_PREFIX}/dags/{dag_id}/dagRuns/{run_id}")

    def list_dag_runs(
        self,
        dag_id: str,
        limit: int = 10,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit, "order_by": "-start_date"}
        if state:
            params["state"] = state
        response = self._get_json(
            f"{_API_PREFIX}/dags/{dag_id}/dagRuns",
            params=params,
        )
        return cast(list[dict[str, Any]], response.get("dag_runs", []))

    def list_all_dag_runs(
        self,
        dag_ids: list[str],
        limit: int = 5,
    ) -> dict[str, list[dict[str, Any]]]:
        return {dag_id: self.list_dag_runs(dag_id, limit=limit) for dag_id in dag_ids}

    def list_task_instances(
        self,
        dag_id: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        response = self._get_json(
            f"{_API_PREFIX}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
        )
        return cast(list[dict[str, Any]], response.get("task_instances", []))

    def get_task_log(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int = 1,
    ) -> str:
        return self._get_text(
            f"{_API_PREFIX}/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}",
            headers={"Accept": "text/plain"},
        )

    def stream_task_log(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int = 1,
    ) -> Iterator[str]:
        """Stream task log lines."""
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
            yield from response.iter_lines()

    def health(self) -> dict[str, Any]:
        return self._get_json(f"{_API_PREFIX}/health")

    def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | str:
        try:
            response = self._client.get(path, params=params, headers=headers)
            self._raise_for_status(response)
            if "json" in response.headers.get("content-type", ""):
                return cast(dict[str, Any], response.json())
            return response.text
        except httpx.RequestError as exc:
            raise OrchestratorError(f"GET {path}", cause=exc) from exc

    def _get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        result = self._get(path, params=params, headers=headers)
        if isinstance(result, str):
            raise OrchestratorError(f"Expected JSON response for GET {path}")
        return result

    def _get_text(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        result = self._get(path, params=params, headers=headers)
        if isinstance(result, str):
            return result
        return str(result)

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self._client.post(path, json=body)
            self._raise_for_status(response)
            return cast(dict[str, Any], response.json())
        except httpx.RequestError as exc:
            raise OrchestratorError(f"POST {path}", cause=exc) from exc

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
        self._client.close()
