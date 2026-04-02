# Development Overrides

These files are opt-in developer helpers only.

- They are not part of the authoritative OAI + OVS + ONOS N6 slicing deployment.
- The journal-grade deployment path uses only `docker-compose.yaml` and `.env` from the parent directory.
- If you use one of these files, do so explicitly with `docker compose -f docker-compose.yaml -f overrides/<file> ...`.

Available overrides:

- `local-dev.override.yaml`: mounts locally rebuilt gNB and UE binaries.
- `ue-gdb.override.yaml`: starts the first UE under `gdb`.
