from __future__ import annotations

import copy
from typing import Any, Dict

from policy_manager.utils import parse_ovs_map, profile_signature, run_command


class OvsClient:
    def __init__(
        self,
        container_name: str,
        bridge_name: str,
        egress_port_name: str,
        dry_run_only: bool = True,
    ) -> None:
        self.container_name = container_name
        self.bridge_name = bridge_name
        self.egress_port_name = egress_port_name
        self.dry_run_only = dry_run_only

    def docker_exec(self, *args: str) -> Dict[str, Any]:
        return run_command(["docker", "exec", self.container_name, *args], timeout_seconds=15)

    def get_interface_ofport(self, port_name: str) -> Dict[str, Any]:
        result = self.docker_exec("ovs-vsctl", "get", "interface", port_name, "ofport")
        if not result["ok"]:
            return result
        raw_value = str(result.get("stdout", "")).strip().strip('"')
        if raw_value in {"", "[]", "{}"}:
            return {"ok": False, "error": f"Could not resolve OpenFlow port number for {port_name}."}
        try:
            ofport = str(int(raw_value))
        except ValueError:
            return {"ok": False, "error": f"Invalid OpenFlow port value for {port_name}: {raw_value}"}
        return {"ok": True, "ofport": ofport}

    def dump_flows(self) -> Dict[str, Any]:
        return self.docker_exec("ovs-ofctl", "-O", "OpenFlow13", "dump-flows", self.bridge_name)

    def ensure_slice_queue_assignment_flows(
        self,
        *,
        upf_port_name: str,
        slice_flow_rules: list[Dict[str, Any]],
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        upf_port_result = self.get_interface_ofport(upf_port_name)
        if not upf_port_result["ok"]:
            if self.dry_run_only:
                upf_port_result = {"ok": True, "ofport": f"<ofport:{upf_port_name}>"}
            else:
                return {
                    "ok": False,
                    "scope": "queue_assignment",
                    "error": upf_port_result.get("error", "Failed to resolve the UPF OpenFlow port."),
                }
        edn_port_result = self.get_interface_ofport(self.egress_port_name)
        if not edn_port_result["ok"]:
            if self.dry_run_only:
                edn_port_result = {"ok": True, "ofport": f"<ofport:{self.egress_port_name}>"}
            else:
                return {
                    "ok": False,
                    "scope": "queue_assignment",
                    "error": edn_port_result.get("error", "Failed to resolve the EDN OpenFlow port."),
                }

        upf_ofport = str(upf_port_result["ofport"])
        edn_ofport = str(edn_port_result["ofport"])
        dump_result = None if self.dry_run_only else self.dump_flows()
        if dump_result is not None and not dump_result["ok"]:
            return {
                "ok": False,
                "scope": "queue_assignment",
                "error": dump_result.get("error", "Failed to inspect current OVS flows."),
                "result": dump_result,
            }

        existing_lines = str((dump_result or {}).get("stdout", "")).splitlines()
        planned_commands: list[str] = []
        command_results: list[Dict[str, Any]] = []
        installed_flows: list[Dict[str, Any]] = []
        existing_flows: list[Dict[str, Any]] = []
        errors: list[str] = []

        for rule in slice_flow_rules:
            udp_port = int(rule["udp_port"])
            queue_id = int(rule["queue_id"])
            priority = int(rule["priority"])
            selector_tokens = [
                f"priority={priority}",
                f"in_port={upf_ofport}",
                "udp",
                f"tp_dst={udp_port}",
            ]
            action_token = f"actions=set_queue:{queue_id},output:{edn_ofport}"
            selector_matches = [
                line for line in existing_lines if all(token in line for token in selector_tokens)
            ]
            exact_exists = any(action_token in line for line in selector_matches)
            conflicting_exists = any(action_token not in line for line in selector_matches)
            if exact_exists and not conflicting_exists and not force_refresh:
                existing_flows.append(
                    {
                        "udp_port": udp_port,
                        "queue_id": queue_id,
                        "priority": priority,
                        "upf_ofport": upf_ofport,
                        "edn_ofport": edn_ofport,
                    }
                )
                continue

            selector = f"priority={priority},in_port={upf_ofport},udp,tp_dst={udp_port}"
            flow_definition = (
                f"priority={priority},in_port={upf_ofport},udp,tp_dst={udp_port},"
                f"actions=set_queue:{queue_id},output:{edn_ofport}"
            )
            delete_command = [
                "docker",
                "exec",
                self.container_name,
                "ovs-ofctl",
                "-O",
                "OpenFlow13",
                "--strict",
                "del-flows",
                self.bridge_name,
                selector,
            ]
            add_command = [
                "docker",
                "exec",
                self.container_name,
                "ovs-ofctl",
                "-O",
                "OpenFlow13",
                "add-flow",
                self.bridge_name,
                flow_definition,
            ]

            if self.dry_run_only:
                planned_commands.append(" ".join(delete_command))
                planned_commands.append(" ".join(add_command))
                installed_flows.append(
                    {
                        "udp_port": udp_port,
                        "queue_id": queue_id,
                        "priority": priority,
                        "upf_ofport": upf_ofport,
                        "edn_ofport": edn_ofport,
                    }
                )
                continue

            delete_result = run_command(delete_command, timeout_seconds=15)
            add_result = run_command(add_command, timeout_seconds=15)
            command_results.extend([delete_result, add_result])
            if add_result["ok"]:
                installed_flows.append(
                    {
                        "udp_port": udp_port,
                        "queue_id": queue_id,
                        "priority": priority,
                        "upf_ofport": upf_ofport,
                        "edn_ofport": edn_ofport,
                    }
                )
                continue
            errors.append(add_result.get("error", "OVS queue assignment flow update failed."))

        result: Dict[str, Any] = {
            "ok": not errors,
            "scope": "queue_assignment",
            "bridge_name": self.bridge_name,
            "upf_ofport": upf_ofport,
            "edn_ofport": edn_ofport,
            "installed_flows": installed_flows,
            "existing_flows": existing_flows,
            "dry_run": self.dry_run_only,
            "error": "; ".join(errors),
        }
        if self.dry_run_only:
            result["planned_commands"] = planned_commands
            result["planned_only"] = True
        else:
            result["results"] = command_results
        return result

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
