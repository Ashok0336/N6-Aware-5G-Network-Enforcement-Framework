from __future__ import annotations

import copy
from typing import Any, Dict

from policy_manager.utils import parse_ovs_map, profile_signature, run_command


class OvsClient:
    def __init__(self, container_name: str, egress_port_name: str, dry_run_only: bool = True) -> None:
        self.container_name = container_name
        self.egress_port_name = egress_port_name
        self.dry_run_only = dry_run_only

    def docker_exec(self, *args: str) -> Dict[str, Any]:
        return run_command(["docker", "exec", self.container_name, *args], timeout_seconds=15)

    def get_port_qos_uuid(self) -> Dict[str, Any]:
        result = self.docker_exec("ovs-vsctl", "get", "port", self.egress_port_name, "qos")
        if not result["ok"]:
            return result
        qos_uuid = str(result.get("stdout", "")).strip()
        if qos_uuid in {"", "[]", "{}"}:
            return {"ok": False, "error": "No QoS object is attached to the egress port."}
        return {"ok": True, "qos_uuid": qos_uuid}

    def read_current_queue_profile(self) -> Dict[str, Any]:
        qos_result = self.get_port_qos_uuid()
        if not qos_result["ok"]:
            return qos_result
        qos_uuid = str(qos_result["qos_uuid"])
        parent_result = self.docker_exec("ovs-vsctl", "get", "qos", qos_uuid, "other-config")
        queue_map_result = self.docker_exec("ovs-vsctl", "get", "qos", qos_uuid, "queues")
        if not parent_result["ok"] or not queue_map_result["ok"]:
            return {"ok": False, "error": "Failed to read current OVS queue profile."}
        parent_map = parse_ovs_map(parent_result["stdout"])
        queue_map = parse_ovs_map(queue_map_result["stdout"])
        profile = {
            "parent_max_rate_bps": int(parent_map.get("max-rate", "0") or "0"),
            "queues": {},
        }
        for queue_id, queue_uuid in queue_map.items():
            queue_result = self.docker_exec("ovs-vsctl", "get", "queue", queue_uuid, "other-config")
            if not queue_result["ok"]:
                return {"ok": False, "error": f"Failed to read queue {queue_id}."}
            queue_cfg = parse_ovs_map(queue_result["stdout"])
            profile["queues"][str(queue_id)] = {
                "min_rate_bps": int(queue_cfg.get("min-rate", "0") or "0"),
                "max_rate_bps": int(queue_cfg.get("max-rate", "0") or "0"),
            }
        return {"ok": True, "profile": profile, "signature": profile_signature(profile)}

    def apply_queue_profile(self, target_profile: Dict[str, Any]) -> Dict[str, Any]:
        qos_result = self.get_port_qos_uuid()
        if not qos_result["ok"]:
            return self._create_queue_profile(target_profile)

        qos_uuid = str(qos_result["qos_uuid"])
        queue_map_result = self.docker_exec("ovs-vsctl", "get", "qos", qos_uuid, "queues")
        if not queue_map_result["ok"]:
            return self._create_queue_profile(target_profile)
        queue_uuid_map = parse_ovs_map(queue_map_result["stdout"])
        for queue_id in target_profile["queues"]:
            if str(queue_id) not in queue_uuid_map:
                return self._create_queue_profile(target_profile)
        return self._update_queue_profile(target_profile, qos_uuid, queue_uuid_map)

    def _create_queue_profile(self, target_profile: Dict[str, Any]) -> Dict[str, Any]:
        queue_ids = sorted(target_profile["queues"], key=lambda item: int(item))
        command = [
            "docker",
            "exec",
            self.container_name,
            "ovs-vsctl",
            "--if-exists",
            "clear",
            "port",
            self.egress_port_name,
            "qos",
            "--",
            "set",
            "port",
            self.egress_port_name,
            "qos=@newqos",
            "--",
            "--id=@newqos",
            "create",
            "qos",
            "type=linux-htb",
            f"other-config:max-rate={target_profile['parent_max_rate_bps']}",
        ]
        for queue_id in queue_ids:
            command.append(f"queues:{queue_id}=@q{queue_id}")
        for queue_id in queue_ids:
            queue_cfg = target_profile["queues"][queue_id]
            command.extend(
                [
                    "--",
                    f"--id=@q{queue_id}",
                    "create",
                    "queue",
                    f"other-config:min-rate={queue_cfg['min_rate_bps']}",
                    f"other-config:max-rate={queue_cfg['max_rate_bps']}",
                ]
            )
        if self.dry_run_only:
            return {"ok": True, "dry_run": True, "planned_commands": [" ".join(command)]}
        result = run_command(command, timeout_seconds=20)
        return {"ok": result["ok"], "dry_run": False, "result": result}

    def _update_queue_profile(
        self, target_profile: Dict[str, Any], qos_uuid: str, queue_uuid_map: Dict[str, str]
    ) -> Dict[str, Any]:
        commands = [
            [
                "docker",
                "exec",
                self.container_name,
                "ovs-vsctl",
                "set",
                "qos",
                qos_uuid,
                f"other-config:max-rate={target_profile['parent_max_rate_bps']}",
            ]
        ]
        for queue_id, queue_cfg in sorted(target_profile["queues"].items(), key=lambda item: int(item[0])):
            commands.append(
                [
                    "docker",
                    "exec",
                    self.container_name,
                    "ovs-vsctl",
                    "set",
                    "queue",
                    queue_uuid_map[str(queue_id)],
                    f"other-config:min-rate={queue_cfg['min_rate_bps']}",
                    f"other-config:max-rate={queue_cfg['max_rate_bps']}",
                ]
            )
        if self.dry_run_only:
            return {"ok": True, "dry_run": True, "planned_commands": [" ".join(command) for command in commands]}
        results = [run_command(command, timeout_seconds=20) for command in commands]
        return {"ok": all(result["ok"] for result in results), "dry_run": False, "results": results}
