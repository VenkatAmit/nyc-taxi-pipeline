"""
Tests for cli/airflow_client.py.

httpx is mocked throughout — no real Airflow instance required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cli.airflow_client import AirflowClient
from pipeline.exceptions import OrchestratorError
from pipeline.settings import AirflowSettings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(monkeypatch: pytest.MonkeyPatch) -> AirflowSettings:
    monkeypatch.setenv("AIRFLOW_API_URL", "http://localhost:8080")
    monkeypatch.setenv("AIRFLOW_USERNAME", "admin")
    monkeypatch.setenv("AIRFLOW_PASSWORD", "admin")
    return AirflowSettings()


@pytest.fixture()
def client(settings: AirflowSettings) -> AirflowClient:
    return AirflowClient(settings=settings)


def _mock_response(
    status_code: int = 200,
    json_data: Any = None,
    text: str = "",
    content_type: str = "application/json",
) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data or {}
    response.text = text
    response.headers = {"content-type": content_type}
    response.request = MagicMock()
    response.request.method = "GET"
    response.request.url = "http://localhost:8080/api/v1/test"
    return response


# ---------------------------------------------------------------------------
# trigger_dag
# ---------------------------------------------------------------------------


class TestTriggerDag:
    def test_successful_trigger(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(
            json_data={"dag_run_id": "manual__2024-01-01", "state": "queued"}
        )
        with patch.object(client._client, "post", return_value=mock_resp):
            result = client.trigger_dag("bronze_dag")
        assert result["dag_run_id"] == "manual__2024-01-01"

    def test_trigger_with_logical_date(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(json_data={"dag_run_id": "scheduled__2024-01-01"})
        with patch.object(client._client, "post", return_value=mock_resp) as mock_post:
            client.trigger_dag("bronze_dag", logical_date="2024-01-01T00:00:00+00:00")
        call_kwargs = mock_post.call_args[1]["json"]
        assert "logical_date" in call_kwargs

    def test_trigger_with_conf(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(json_data={"dag_run_id": "manual__2024-01-01"})
        with patch.object(client._client, "post", return_value=mock_resp) as mock_post:
            client.trigger_dag("bronze_dag", conf={"key": "value"})
        call_kwargs = mock_post.call_args[1]["json"]
        assert call_kwargs["conf"] == {"key": "value"}

    def test_404_raises_orchestrator_error(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(status_code=404)
        with patch.object(client._client, "post", return_value=mock_resp):
            with pytest.raises(OrchestratorError) as exc_info:
                client.trigger_dag("nonexistent_dag")
        assert exc_info.value.status_code == 404

    def test_network_error_raises_orchestrator_error(
        self, client: AirflowClient
    ) -> None:
        import httpx

        with patch.object(
            client._client, "post", side_effect=httpx.ConnectError("refused")
        ):
            with pytest.raises(OrchestratorError):
                client.trigger_dag("bronze_dag")


# ---------------------------------------------------------------------------
# get_dag_run
# ---------------------------------------------------------------------------


class TestGetDagRun:
    def test_returns_run_data(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(
            json_data={"dag_run_id": "run_1", "state": "success"}
        )
        with patch.object(client._client, "get", return_value=mock_resp):
            result = client.get_dag_run("bronze_dag", "run_1")
        assert result["state"] == "success"


# ---------------------------------------------------------------------------
# list_dag_runs
# ---------------------------------------------------------------------------


class TestListDagRuns:
    def test_returns_list(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(
            json_data={"dag_runs": [{"dag_run_id": "r1"}, {"dag_run_id": "r2"}]}
        )
        with patch.object(client._client, "get", return_value=mock_resp):
            runs = client.list_dag_runs("bronze_dag")
        assert len(runs) == 2

    def test_empty_list_on_no_runs(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(json_data={"dag_runs": []})
        with patch.object(client._client, "get", return_value=mock_resp):
            runs = client.list_dag_runs("bronze_dag")
        assert runs == []

    def test_state_filter_passed_as_param(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(json_data={"dag_runs": []})
        with patch.object(client._client, "get", return_value=mock_resp) as mock_get:
            client.list_dag_runs("bronze_dag", state="success")
        params = mock_get.call_args[1]["params"]
        assert params["state"] == "success"


# ---------------------------------------------------------------------------
# list_task_instances
# ---------------------------------------------------------------------------


class TestListTaskInstances:
    def test_returns_task_instances(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(
            json_data={
                "task_instances": [
                    {"task_id": "ingest_trips", "state": "success"},
                    {"task_id": "validate_trips", "state": "success"},
                ]
            }
        )
        with patch.object(client._client, "get", return_value=mock_resp):
            instances = client.list_task_instances("bronze_dag", "run_1")
        assert len(instances) == 2
        assert instances[0]["task_id"] == "ingest_trips"


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_returns_health_dict(self, client: AirflowClient) -> None:
        mock_resp = _mock_response(
            json_data={
                "metadatabase": {"status": "healthy"},
                "scheduler": {"status": "healthy"},
            }
        )
        with patch.object(client._client, "get", return_value=mock_resp):
            result = client.health()
        assert result["metadatabase"]["status"] == "healthy"


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_close_called_on_exit(self, client: AirflowClient) -> None:
        with patch.object(client._client, "close") as mock_close:
            with client:
                pass
        mock_close.assert_called_once()
