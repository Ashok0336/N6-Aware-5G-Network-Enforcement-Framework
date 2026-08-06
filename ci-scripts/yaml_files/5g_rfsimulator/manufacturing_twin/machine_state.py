#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional


Number = Optional[float]


@dataclass
class MachineTwinState:
    timestamp: str
    machine_id: str
    machine_type: str
    controller: str
    octoprint_url: str = ""
    octoprint_reachable: bool = False
    printer_operational: bool = False
    availability: str = "unknown"
    printer_state_text: Optional[str] = None
    job_state: Optional[str] = None
    job_file: Optional[str] = None
    job_progress_percent: Number = None
    print_time_seconds: Number = None
    print_time_left_seconds: Number = None
    nozzle_actual_c: Number = None
    nozzle_target_c: Number = None
    bed_actual_c: Number = None
    bed_target_c: Number = None
    manufacturing_phase: str = "unknown"
    service_criticality: Dict[str, str] = field(default_factory=dict)
    state_age_seconds: Number = None
    api_error: Optional[str] = None
    raw_octoprint: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "MachineTwinState":
        return cls(**payload)
