#!/usr/bin/env python3
"""Generates dynamic docker-compose YAML files for arbitrary cluster sizes (N nodes)."""

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "compose.dynamic.yaml"


def generate_compose_content(
    num_nodes: int,
    coordinator_id: int | None = None,
    rpc_timeout: float = 2.0,
    heartbeat_interval: float = 3.0,
    failure_threshold: int = 2,
    base_port: int = 50050,
) -> str:
    """Builds the string content of a Docker Compose YAML for N nodes.

    Args:
        num_nodes: Total number of nodes (e.g. 3, 5, 7).
        coordinator_id: Initial coordinator ID. If None, uses num_nodes (highest ID).
                        Set to 0 or negative for cold-start (no initial coordinator).
        rpc_timeout: Default RPC deadline in seconds.
        heartbeat_interval: Default heartbeat interval in seconds.
        failure_threshold: Consecutive failures required to start election.
        base_port: Base host port for port mapping (e.g. 50050 -> node1 on 50051).

    Returns:
        Formatted YAML string ready to write to disk.
    """
    if num_nodes < 2:
        raise ValueError("A distributed cluster must have at least 2 nodes.")

    # All nodes know about all other nodes in the network:
    endpoints = ",".join(f"{i}=node{i}:50051" for i in range(1, num_nodes + 1))

    # By default in Bully, the highest ID is the initial leader (unless cold-start):
    if coordinator_id is None:
        coord_val = str(num_nodes)
    elif coordinator_id <= 0:
        coord_val = ""  # Cold start: no coordinator initially
    else:
        coord_val = str(coordinator_id)

    services_lines = ["services:"]
    for i in range(1, num_nodes + 1):
        host_port = base_port + i
        service_block = f"""  node{i}:
    build: .
    environment:
      NODE_ID: "{i}"
      NODE_PORT: "50051"
      COORDINATOR_ID: "{coord_val}"
      NODE_ENDPOINTS: "{endpoints}"
      RPC_TIMEOUT_SECONDS: "${{RPC_TIMEOUT_SECONDS:-{rpc_timeout}}}"
      HEARTBEAT_INTERVAL_SECONDS: "${{HEARTBEAT_INTERVAL_SECONDS:-{heartbeat_interval}}}"
      HEARTBEAT_FAILURE_THRESHOLD: "${{HEARTBEAT_FAILURE_THRESHOLD:-{failure_threshold}}}"
    ports:
      - "{host_port}:50051"
"""
        services_lines.append(service_block)

    return "\n".join(services_lines)


def write_dynamic_compose(
    num_nodes: int,
    output_path: Path = DEFAULT_OUTPUT,
    coordinator_id: int | None = None,
    rpc_timeout: float = 2.0,
    heartbeat_interval: float = 3.0,
    failure_threshold: int = 2,
) -> Path:
    """Generates and writes a compose.dynamic.yaml file to disk."""
    content = generate_compose_content(
        num_nodes=num_nodes,
        coordinator_id=coordinator_id,
        rpc_timeout=rpc_timeout,
        heartbeat_interval=heartbeat_interval,
        failure_threshold=failure_threshold,
    )
    output_path.write_text(content, encoding="utf-8")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate dynamic docker compose for N nodes in Bully election."
    )
    parser.add_argument(
        "-n",
        "--nodes",
        type=int,
        default=5,
        help="Number of nodes to generate in the cluster (default: 5)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output YAML file path (default: {DEFAULT_OUTPUT.name})",
    )
    parser.add_argument(
        "--cold-start",
        action="store_true",
        help="Start with no initial coordinator (COORDINATOR_ID='') to force election from scratch",
    )
    parser.add_argument(
        "--coordinator",
        type=int,
        default=None,
        help="Custom initial coordinator ID (default: highest node ID)",
    )
    args = parser.parse_args()

    coord_id = 0 if args.cold_start else args.coordinator
    out = write_dynamic_compose(
        num_nodes=args.nodes,
        output_path=args.output,
        coordinator_id=coord_id,
    )
    mode = "Cold-Start (sem líder)" if args.cold_start else f"Líder inicial: Nó {args.nodes}"
    print(f"✅ Arquivo gerado com sucesso: {out}")
    print(f"   - Total de nós: {args.nodes}")
    print(f"   - Modo: {mode}")
    print(f"   - Para subir o cluster:  docker compose -f {out.name} up -d")
    print(f"   - Para acompanhar logs:  docker compose -f {out.name} logs -f")
    print(f"   - Para derrubar:         docker compose -f {out.name} down")


if __name__ == "__main__":
    main()
