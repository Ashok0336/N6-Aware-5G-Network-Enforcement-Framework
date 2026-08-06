#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests


class OctoPrintAdapter:
    """Read-only OctoPrint REST adapter for telemetry collection."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.base_url = (base_url or os.environ.get("OCTOPRINT_URL") or "").rstrip("/")
        self.api_key = api_key or os.environ.get("OCTOPRINT_API_KEY") or ""
        self.timeout_seconds = timeout_seconds

    def get_version(self) -> Dict[str, Any]:
        return self._get("/api/version")

    def get_printer(self) -> Dict[str, Any]:
        return self._get("/api/printer")

    def get_job(self) -> Dict[str, Any]:
        return self._get("/api/job")

    def get_server_version(self) -> Dict[str, Any]:
        return self.get_version()

    def get_printer_state(self) -> Dict[str, Any]:
        return self.get_printer()

    def get_job_state(self) -> Dict[str, Any]:
        return self.get_job()

    def read_state(self) -> Dict[str, Any]:
        if not self.base_url or not self.api_key:
            return self._base_state(
                octoprint_reachable=False,
                printer_operational=False,
                availability="octoprint_unreachable",
                api_error="missing OCTOPRINT_URL or OCTOPRINT_API_KEY",
            )

        version = self.get_version()
        if not version.get("ok"):
            return self._base_state(
                octoprint_reachable=False,
                printer_operational=False,
                availability="octoprint_unreachable",
                api_error=str(version.get("error") or "OctoPrint version API unavailable"),
                version=version.get("data"),
            )

        printer = self.get_printer()
        job = self.get_job()
        printer_data = printer.get("data") if isinstance(printer.get("data"), dict) else {}
        job_data = job.get("data") if isinstance(job.get("data"), dict) else {}
        printer_state = printer_data.get("state") if isinstance(printer_data.get("state"), dict) else {}
        printer_flags = printer_state.get("flags") if isinstance(printer_state.get("flags"), dict) else {}
        printer_state_text = _string_or_none(printer_state.get("text"))
        job_state = _string_or_none(job_data.get("state"))
        api_error = _join_errors(
            _api_error(printer),
            _api_error(job),
            _payload_error(printer_data),
            _payload_error(job_data),
        )
        printer_operational = bool(printer.get("ok"))
        if str(job_state or "").lower() == "offline":
            printer_operational = False
        if _payload_error(printer_data).lower() == "printer is not operational":
            printer_operational = False
        if printer.get("status_code") == 403:
            printer_operational = False
        if printer_flags and printer_flags.get("operational") is False:
            printer_operational = False

        availability = "available" if printer_operational else "printer_offline"
        return self._base_state(
            octoprint_reachable=True,
            printer_operational=printer_operational,
            availability=availability,
            api_error=api_error,
            version=version.get("data", {}),
            printer=printer_data,
            job=job_data,
            printer_state_text=printer_state_text,
            job_state=job_state,
        )

    def _get(self, endpoint: str) -> Dict[str, Any]:
        if not self.base_url:
            return {"ok": False, "error": "missing OCTOPRINT_URL"}
        if not self.api_key:
            return {"ok": False, "error": "missing OCTOPRINT_API_KEY"}

        url = urljoin(f"{self.base_url}/", endpoint.lstrip("/"))
        headers = {"X-Api-Key": self.api_key}
        try:
            response = requests.get(url, headers=headers, timeout=self.timeout_seconds)
            data: Any
            try:
                data = response.json()
            except ValueError:
                data = {}
            if response.status_code >= 400:
                return {
                    "ok": False,
                    "status_code": response.status_code,
                    "data": data if isinstance(data, dict) else {},
                    "error": _payload_error(data) or f"HTTP {response.status_code} reading {endpoint}",
                }
            return {
                "ok": True,
                "status_code": response.status_code,
                "data": data if isinstance(data, dict) else {},
                "error": None,
            }
        except requests.Timeout:
            return {"ok": False, "error": f"timeout reading {endpoint}"}
        except requests.RequestException as exc:
            return {"ok": False, "error": f"request failed for {endpoint}: {exc}"}

    def _base_state(
        self,
        *,
        octoprint_reachable: bool,
        printer_operational: bool,
        availability: str,
        api_error: str | None,
        version: Any = None,
        printer: Any = None,
        job: Any = None,
        printer_state_text: str | None = None,
        job_state: str | None = None,
    ) -> Dict[str, Any]:
        return {
            "timestamp": _utc_timestamp(),
            "octoprint_url": self.base_url,
            "octoprint_reachable": octoprint_reachable,
            "printer_operational": printer_operational,
            "availability": availability,
            "status": availability,
            "printer_state_text": printer_state_text,
            "job_state": job_state,
            "api_error": api_error,
            "version": version,
            "printer": printer if isinstance(printer, dict) else {},
            "job": job if isinstance(job, dict) else {},
        }


def read_state() -> Dict[str, Any]:
    return OctoPrintAdapter().read_state()


def _payload_error(payload: Any) -> str:
    if isinstance(payload, dict) and payload.get("error"):
        return str(payload["error"])
    return ""


def _api_error(result: Dict[str, Any]) -> str:
    return str(result.get("error") or "")


def _join_errors(*errors: str) -> str | None:
    cleaned = [error for error in errors if error]
    return "; ".join(dict.fromkeys(cleaned)) if cleaned else None


def _string_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
