from pipeline.settings import AirflowSettings

__all__ = ["AirflowSettings", "get_airflow_settings"]


def get_airflow_settings() -> AirflowSettings:
    """Return AirflowSettings from environment variables."""
    return AirflowSettings.model_validate({})  # reads from env via pydantic-settings
