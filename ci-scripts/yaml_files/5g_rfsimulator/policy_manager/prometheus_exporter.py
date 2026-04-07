from __future__ import annotations

import datetime as dt
from typing import Any, Iterable, Optional

try:
    from prometheus_client import Counter, Gauge, start_http_server
except ImportError as exc:  # pragma: no cover - dependency checked at runtime
    Counter = None  # type: ignore[assignment]
    Gauge = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]
    PROMETHEUS_IMPORT_ERROR: Optional[ImportError] = exc
else:
    PROMETHEUS_IMPORT_ERROR = None


DEFAULT_METRICS_HTTP_HOST = "0.0.0.0"
DEFAULT_METRICS_HTTP_PORT = 8001
MAINTAIN_CURRENT_POLICY = "MAINTAIN_CURRENT_POLICY"


def _iso_timestamp_to_unix(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return dt.datetime.now(dt.timezone.utc).timestamp()
    try:
        normalized = text.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return dt.datetime.now(dt.timezone.utc).timestamp()


class PolicyPrometheusExporter:
    def __init__(self, metrics_http_host: str, metrics_http_port: int) -> None:
        self.metrics_http_host = str(metrics_http_host or DEFAULT_METRICS_HTTP_HOST)
        self.metrics_http_port = int(metrics_http_port or DEFAULT_METRICS_HTTP_PORT)
        self.metrics_path = "/metrics"
        self.metrics_bind_label = f"{self.metrics_http_host}:{self.metrics_http_port}"
        self.metrics_access_url = f"http://127.0.0.1:{self.metrics_http_port}{self.metrics_path}"
        self.server_started = False
        self._register_metrics()

    def _register_metrics(self) -> None:
        if Gauge is None or Counter is None:
            return
        self.prometheus_policy_cycle_total = Counter(
            "policy_cycle_total",
            "Total policy cycles processed by the policy manager.",
        )
        self.prometheus_policy_decisions_total = Counter(
            "policy_decisions_total",
            "Total slice decisions evaluated by the policy manager.",
            ["slice", "recommended_action", "healthy"],
        )
        self.prometheus_policy_slice_decisions_total = Counter(
            "policy_slice_decisions_total",
            "Per-slice policy decision counters.",
            ["slice"],
        )
        self.prometheus_policy_actions_applied_total = Counter(
            "policy_actions_applied_total",
            "Total policy actions successfully applied by the policy manager.",
            ["slice", "recommended_action", "mode"],
        )
        self.prometheus_policy_last_decision_timestamp = Gauge(
            "policy_last_decision_timestamp",
            "Unix timestamp of the latest decision for each slice.",
            ["slice"],
        )
        self.prometheus_policy_dry_run_mode = Gauge(
            "policy_dry_run_mode",
            "Policy manager dry-run mode state. 1 means dry-run, 0 means active.",
        )
        self.prometheus_slice_policy_state = Gauge(
            "slice_policy_state",
            "Current policy state per slice and recommended action.",
            ["slice", "recommended_action", "healthy", "mode"],
        )

    def start(self) -> None:
        if self.server_started:
            return
        if PROMETHEUS_IMPORT_ERROR is not None or start_http_server is None:
            raise RuntimeError(
                "prometheus_client is required for the policy manager metrics endpoint. "
                "Install it before running the policy manager."
            ) from PROMETHEUS_IMPORT_ERROR
        start_http_server(self.metrics_http_port, addr=self.metrics_http_host)
        self.server_started = True

    def update_cycle(
        self,
        *,
        cycle_timestamp: Any,
        dry_run_only: bool,
        decisions: Iterable[Any],
        enforcement_result: dict[str, Any],
    ) -> None:
        if Gauge is None or Counter is None:
            raise RuntimeError(
                "prometheus_client is required for Prometheus metrics export."
            )

        self.prometheus_policy_cycle_total.inc()
        self.prometheus_policy_dry_run_mode.set(1.0 if dry_run_only else 0.0)
        self.prometheus_slice_policy_state.clear()

        decision_timestamp = _iso_timestamp_to_unix(cycle_timestamp)
        mode_label = "dry-run" if dry_run_only else str(enforcement_result.get("mode") or "active")

        for decision in decisions:
            slice_name = str(getattr(decision, "slice_name", "") or "unknown")
            recommended_action = str(
                getattr(decision, "recommended_action", "") or MAINTAIN_CURRENT_POLICY
            )
            healthy = bool(getattr(decision, "healthy", False))
            healthy_label = "true" if healthy else "false"

            self.prometheus_policy_decisions_total.labels(
                slice=slice_name,
                recommended_action=recommended_action,
                healthy=healthy_label,
            ).inc()
            self.prometheus_policy_slice_decisions_total.labels(slice=slice_name).inc()
            self.prometheus_policy_last_decision_timestamp.labels(slice=slice_name).set(
                decision_timestamp
            )
            self.prometheus_slice_policy_state.labels(
                slice=slice_name,
                recommended_action=recommended_action,
                healthy=healthy_label,
                mode=mode_label,
            ).set(1.0)

            if (
                bool(enforcement_result.get("applied"))
                and recommended_action != MAINTAIN_CURRENT_POLICY
            ):
                self.prometheus_policy_actions_applied_total.labels(
                    slice=slice_name,
                    recommended_action=recommended_action,
                    mode=mode_label,
                ).inc()
