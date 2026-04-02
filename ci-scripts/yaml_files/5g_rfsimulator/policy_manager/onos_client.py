from __future__ import annotations

from typing import Any, Dict, List

from automation.onos_client import OnosClient as BaseOnosClient


class OnosClient(BaseOnosClient):
    def ensure_baseline_slice_flows(
        self,
        *,
        devices_path: str,
        upf_port_name: str,
        edn_port_name: str,
        slice_flow_rules: List[Dict[str, Any]],
        base_forward_flow_priority: int = 5000,
        reverse_flow_priority: int = 20000,
        arp_flow_priority: int = 45000,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        return self.ensure_slice_queue_flows(
            devices_path=devices_path,
            upf_port_name=upf_port_name,
            edn_port_name=edn_port_name,
            flow_rules=slice_flow_rules,
            base_forward_flow_priority=base_forward_flow_priority,
            reverse_flow_priority=reverse_flow_priority,
            arp_flow_priority=arp_flow_priority,
            force_refresh=force_refresh,
        )
