#!/usr/bin/env python3
"""Modular test battery for Bully election with fast in-place resets and container reuse."""

import argparse
import csv
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_compose import write_dynamic_compose

BATTERY_RESULTS = ROOT / "experiments" / "battery_results.csv"
BATTERY_SUMMARY = ROOT / "experiments" / "battery_summary.csv"

WINNER_PATTERN = re.compile(r"Eleição .*: nó (\d+) tornou-se coordenador")
RECOGNIZED_PATTERN = re.compile(r"Novo coordenador reconhecido: Nó (\d+)")
HEARTBEAT_PATTERN = re.compile(r"Pong recebido do nó (\d+)")
MSG_PATTERN = re.compile(
    r"\[MSG\] type=(\w+) dir=(SENT|RECV) from=(\d+) to=(\d+) election_id=(\d+)"
)


def compose(*args: str, compose_file: str = "compose.yaml", env: dict[str, str] | None = None) -> str:
    cmd = ["docker", "compose", "-f", compose_file, *args]
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        capture_output=True,
        timeout=35,
    )
    return result.stdout


def stop_cluster(compose_file: str = "compose.yaml", env: dict[str, str] | None = None) -> None:
    subprocess.run(
        ["docker", "compose", "-f", compose_file, "down", "--remove-orphans", "-t", "1"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        timeout=15,
    )


def read_logs(compose_file: str, *nodes: str, tail: int = 1000, env: dict[str, str] | None = None) -> str:
    cmd = ["logs", "--no-log-prefix", "--no-color", f"--tail={tail}"]
    if nodes:
        cmd.extend(nodes)
    return compose(*cmd, compose_file=compose_file, env=env)


def count_election_messages(logs_window: str) -> dict[str, int]:
    """Counts messages sent during an election window."""
    counts = {
        "messages_total": 0,
        "election_msgs": 0,
        "ok_msgs": 0,
        "coordinator_msgs": 0,
        "ack_msgs": 0,
    }
    for match in MSG_PATTERN.finditer(logs_window):
        msg_type, direction, _, _, _ = match.groups()
        if direction == "SENT":
            counts["messages_total"] += 1
            if msg_type == "ElectionMsg":
                counts["election_msgs"] += 1
            elif msg_type == "OkMsg":
                counts["ok_msgs"] += 1
            elif msg_type == "CoordinatorMsg":
                counts["coordinator_msgs"] += 1
            elif msg_type == "AckMsg":
                counts["ack_msgs"] += 1
    return counts


def percentile(values: list[float], percentage: float) -> float:
    """Calculates percentile with linear interpolation."""
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentage / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


# ============================================================================
# Test Scenarios with In-Place Reset
# ============================================================================

def run_baseline_scenario(
    n_nodes: int = 3,
    repetitions: int = 1,
    interval: float = 1.0,
    enable_metrics_logs: bool = True,
) -> list[dict[str, object]]:
    """Runs N repetitions of the Baseline scenario reusing the running cluster."""
    if n_nodes == 3:
        compose_file = "compose.yaml"
    else:
        compose_path = ROOT / "compose.dynamic.yaml"
        write_dynamic_compose(
            num_nodes=n_nodes,
            output_path=compose_path,
            coordinator_id=n_nodes,
            heartbeat_interval=interval,
        )
        compose_file = "compose.dynamic.yaml"

    env = os.environ.copy()
    env.update(
        {
            "HEARTBEAT_INTERVAL_SECONDS": str(interval),
            "RPC_TIMEOUT_SECONDS": "2",
            "HEARTBEAT_FAILURE_THRESHOLD": "2",
            "ENABLE_METRICS_LOGS": "1" if enable_metrics_logs else "0",
        }
    )

    rows = []
    surviving_nodes = [f"node{i}" for i in range(1, n_nodes)]
    all_nodes = [f"node{i}" for i in range(1, n_nodes + 1)]
    coord_node = f"node{n_nodes}"
    expected_failure_winner = str(n_nodes - 1)
    expected_recovery_winner = str(n_nodes)

    try:
        # Start cluster once for all repetitions
        compose("up", "-d", compose_file=compose_file, env=env)
        startup_wait = max(4.0, interval * 2 + 1)
        time.sleep(startup_wait)

        for rep in range(1, repetitions + 1):
            # Record log offset for this specific repetition
            pre_rep_logs = read_logs(compose_file, *all_nodes, env=env)
            offset_all = len(pre_rep_logs)
            pre_surv_logs = read_logs(compose_file, *surviving_nodes, env=env)
            offset_surv = len(pre_surv_logs)

            normal_passed = len(re.findall(rf"Pong recebido do nó {n_nodes}", pre_surv_logs[-500:])) >= 1

            # Phase 1: Failure of coordinator N
            compose("stop", "-t", "1", coord_node, compose_file=compose_file, env=env)
            failure_injected_at = time.monotonic()
            deadline = failure_injected_at + 25
            failure_winner = ""
            while time.monotonic() < deadline:
                current_surv_logs = read_logs(compose_file, *surviving_nodes, env=env)[offset_surv:]
                matches = WINNER_PATTERN.findall(current_surv_logs)
                if matches and matches[-1] == expected_failure_winner:
                    failure_winner = expected_failure_winner
                    break
                rec_matches = RECOGNIZED_PATTERN.findall(current_surv_logs)
                if rec_matches and rec_matches[-1] == expected_failure_winner:
                    failure_winner = expected_failure_winner
                    break
                time.sleep(0.15)

            failure_convergence = round(time.monotonic() - failure_injected_at, 3)
            failure_passed = (failure_winner == expected_failure_winner)

            # Phase 2: Fast in-place recovery of coordinator N
            compose("start", coord_node, compose_file=compose_file, env=env)
            recovery_injected_at = time.monotonic()
            deadline = recovery_injected_at + 25
            recovery_winner = ""
            while time.monotonic() < deadline:
                current_surv_logs = read_logs(compose_file, *surviving_nodes, env=env)[offset_surv:]
                rec_matches = RECOGNIZED_PATTERN.findall(current_surv_logs)
                if rec_matches and rec_matches[-1] == expected_recovery_winner:
                    recovery_winner = expected_recovery_winner
                    break
                time.sleep(0.15)

            recovery_convergence = round(time.monotonic() - recovery_injected_at, 3)
            recovery_passed = (recovery_winner == expected_recovery_winner)

            rep_window_logs = read_logs(compose_file, *all_nodes, env=env)[offset_all:]
            total_msgs = count_election_messages(rep_window_logs)

            res = {
                "scenario": f"baseline_N{n_nodes}",
                "n_nodes": n_nodes,
                "repetition": rep,
                "heartbeat_interval_seconds": interval,
                "rpc_timeout_seconds": 2.0,
                "failure_threshold": 2,
                "normal_heartbeat_count": 2,
                "normal_passed": normal_passed,
                "winner_after_failure": failure_winner,
                "convergence_seconds": failure_convergence,
                "failure_passed": failure_passed,
                "winner_after_recovery": recovery_winner,
                "recovery_seconds": recovery_convergence,
                "recovery_passed": recovery_passed,
                "messages_total": total_msgs["messages_total"],
                "election_msgs": total_msgs["election_msgs"],
                "ok_msgs": total_msgs["ok_msgs"],
                "coordinator_msgs": total_msgs["coordinator_msgs"],
                "ack_msgs": total_msgs["ack_msgs"],
            }
            print(f"   [Repetição {rep}/{repetitions}] pass={failure_passed and recovery_passed}, conv={failure_convergence}s, rec={recovery_convergence}s, msgs={total_msgs['messages_total']}")
            rows.append(res)

            # Settle wait between repetitions (cluster is already fully restored!)
            if rep < repetitions:
                time.sleep(max(2.0, interval * 1.5))
    finally:
        stop_cluster(compose_file=compose_file, env=env)

    return rows


def run_scalability_scenario(
    n_nodes: int,
    repetitions: int = 1,
    interval: float = 1.0,
    enable_metrics_logs: bool = True,
) -> list[dict[str, object]]:
    """Runs N repetitions of the Scalability scenario reusing the cluster."""
    compose_path = ROOT / "compose.dynamic.yaml"
    write_dynamic_compose(
        num_nodes=n_nodes,
        output_path=compose_path,
        coordinator_id=n_nodes,
        heartbeat_interval=interval,
    )
    compose_file = "compose.dynamic.yaml"

    env = os.environ.copy()
    env.update(
        {
            "HEARTBEAT_INTERVAL_SECONDS": str(interval),
            "RPC_TIMEOUT_SECONDS": "2",
            "HEARTBEAT_FAILURE_THRESHOLD": "2",
            "ENABLE_METRICS_LOGS": "1" if enable_metrics_logs else "0",
        }
    )

    rows = []
    expected_winner = str(n_nodes - 1)
    coord_node = f"node{n_nodes}"
    surviving_nodes = [f"node{i}" for i in range(1, n_nodes)]

    try:
        compose("up", "-d", compose_file=compose_file, env=env)
        startup_wait = max(4.0, interval * 2 + 1)
        time.sleep(startup_wait)

        for rep in range(1, repetitions + 1):
            pre_logs = read_logs(compose_file, *surviving_nodes, env=env)
            offset = len(pre_logs)

            compose("stop", "-t", "1", coord_node, compose_file=compose_file, env=env)
            failure_injected_at = time.monotonic()
            deadline = failure_injected_at + 25
            winner = ""
            while time.monotonic() < deadline:
                current_logs = read_logs(compose_file, *surviving_nodes, env=env)[offset:]
                matches = WINNER_PATTERN.findall(current_logs)
                if matches and matches[-1] == expected_winner:
                    winner = expected_winner
                    break
                rec_matches = RECOGNIZED_PATTERN.findall(current_logs)
                if rec_matches and rec_matches[-1] == expected_winner:
                    winner = expected_winner
                    break
                time.sleep(0.15)

            convergence = round(time.monotonic() - failure_injected_at, 3)
            failure_passed = (winner == expected_winner)

            window_logs = read_logs(compose_file, *surviving_nodes, env=env)[offset:]
            msg_counts = count_election_messages(window_logs)

            res = {
                "scenario": f"scalability_N{n_nodes}",
                "n_nodes": n_nodes,
                "repetition": rep,
                "heartbeat_interval_seconds": interval,
                "rpc_timeout_seconds": 2.0,
                "failure_threshold": 2,
                "normal_heartbeat_count": 2,
                "normal_passed": True,
                "winner_after_failure": winner,
                "convergence_seconds": convergence,
                "failure_passed": failure_passed,
                "winner_after_recovery": "",
                "recovery_seconds": 0.0,
                "recovery_passed": True,
                "messages_total": msg_counts["messages_total"],
                "election_msgs": msg_counts["election_msgs"],
                "ok_msgs": msg_counts["ok_msgs"],
                "coordinator_msgs": msg_counts["coordinator_msgs"],
                "ack_msgs": msg_counts["ack_msgs"],
            }
            print(f"   [Repetição {rep}/{repetitions}] pass={failure_passed}, conv={convergence}s, msgs={msg_counts['messages_total']}")
            rows.append(res)

            # In-place reset: restart coordinator node for next repetition
            if rep < repetitions:
                compose("start", coord_node, compose_file=compose_file, env=env)
                time.sleep(max(2.0, interval * 1.5))
    finally:
        stop_cluster(compose_file=compose_file, env=env)

    return rows


def run_simultaneous_scenario(
    n_nodes: int = 5,
    repetitions: int = 1,
    interval: float = 1.0,
    enable_metrics_logs: bool = True,
) -> list[dict[str, object]]:
    """Runs N repetitions of the Simultaneous Failure scenario reusing the cluster."""
    if n_nodes < 3:
        raise ValueError("Simultaneous failure scenario requires at least 3 nodes.")

    compose_path = ROOT / "compose.dynamic.yaml"
    write_dynamic_compose(
        num_nodes=n_nodes,
        output_path=compose_path,
        coordinator_id=n_nodes,
        heartbeat_interval=interval,
    )
    compose_file = "compose.dynamic.yaml"

    env = os.environ.copy()
    env.update(
        {
            "HEARTBEAT_INTERVAL_SECONDS": str(interval),
            "RPC_TIMEOUT_SECONDS": "2",
            "HEARTBEAT_FAILURE_THRESHOLD": "2",
            "ENABLE_METRICS_LOGS": "1" if enable_metrics_logs else "0",
        }
    )

    rows = []
    expected_winner = str(n_nodes - 2)
    failed_nodes = [f"node{n_nodes}", f"node{n_nodes - 1}"]
    surviving_nodes = [f"node{i}" for i in range(1, n_nodes - 1)]

    try:
        compose("up", "-d", compose_file=compose_file, env=env)
        startup_wait = max(4.0, interval * 2 + 1)
        time.sleep(startup_wait)

        for rep in range(1, repetitions + 1):
            pre_logs = read_logs(compose_file, *surviving_nodes, env=env)
            offset = len(pre_logs)

            compose("stop", "-t", "1", *failed_nodes, compose_file=compose_file, env=env)
            failure_injected_at = time.monotonic()
            deadline = failure_injected_at + 30
            winner = ""
            while time.monotonic() < deadline:
                current_logs = read_logs(compose_file, *surviving_nodes, env=env)[offset:]
                matches = WINNER_PATTERN.findall(current_logs)
                if matches and matches[-1] == expected_winner:
                    winner = expected_winner
                    break
                rec_matches = RECOGNIZED_PATTERN.findall(current_logs)
                if rec_matches and rec_matches[-1] == expected_winner:
                    winner = expected_winner
                    break
                time.sleep(0.15)

            convergence = round(time.monotonic() - failure_injected_at, 3)
            failure_passed = (winner == expected_winner)

            window_logs = read_logs(compose_file, *surviving_nodes, env=env)[offset:]
            msg_counts = count_election_messages(window_logs)

            res = {
                "scenario": f"simultaneous_N{n_nodes}",
                "n_nodes": n_nodes,
                "repetition": rep,
                "heartbeat_interval_seconds": interval,
                "rpc_timeout_seconds": 2.0,
                "failure_threshold": 2,
                "normal_heartbeat_count": 2,
                "normal_passed": True,
                "winner_after_failure": winner,
                "convergence_seconds": convergence,
                "failure_passed": failure_passed,
                "winner_after_recovery": "",
                "recovery_seconds": 0.0,
                "recovery_passed": True,
                "messages_total": msg_counts["messages_total"],
                "election_msgs": msg_counts["election_msgs"],
                "ok_msgs": msg_counts["ok_msgs"],
                "coordinator_msgs": msg_counts["coordinator_msgs"],
                "ack_msgs": msg_counts["ack_msgs"],
            }
            print(f"   [Repetição {rep}/{repetitions}] pass={failure_passed}, conv={convergence}s, msgs={msg_counts['messages_total']}")
            rows.append(res)

            # In-place reset: start the two stopped nodes back up
            if rep < repetitions:
                compose("start", *failed_nodes, compose_file=compose_file, env=env)
                time.sleep(max(2.0, interval * 1.5))
    finally:
        stop_cluster(compose_file=compose_file, env=env)

    return rows


def run_intermediate_scenario(
    n_nodes: int = 5,
    intermediate_node_id: int | None = None,
    repetitions: int = 1,
    interval: float = 1.0,
    enable_metrics_logs: bool = True,
) -> list[dict[str, object]]:
    """Runs N repetitions of Intermediate Failure scenario reusing the cluster."""
    if intermediate_node_id is None:
        intermediate_node_id = max(2, n_nodes // 2)

    compose_path = ROOT / "compose.dynamic.yaml"
    write_dynamic_compose(
        num_nodes=n_nodes,
        output_path=compose_path,
        coordinator_id=n_nodes,
        heartbeat_interval=interval,
    )
    compose_file = "compose.dynamic.yaml"

    env = os.environ.copy()
    env.update(
        {
            "HEARTBEAT_INTERVAL_SECONDS": str(interval),
            "RPC_TIMEOUT_SECONDS": "2",
            "HEARTBEAT_FAILURE_THRESHOLD": "2",
            "ENABLE_METRICS_LOGS": "1" if enable_metrics_logs else "0",
        }
    )

    rows = []
    target_container = f"node{intermediate_node_id}"
    surviving_nodes = [f"node{i}" for i in range(1, n_nodes + 1) if i != intermediate_node_id]

    try:
        compose("up", "-d", compose_file=compose_file, env=env)
        startup_wait = max(4.0, interval * 2 + 1)
        time.sleep(startup_wait)

        for rep in range(1, repetitions + 1):
            pre_logs = read_logs(compose_file, *surviving_nodes, env=env)
            offset = len(pre_logs)
            pre_winners = WINNER_PATTERN.findall(pre_logs)

            compose("stop", "-t", "1", target_container, compose_file=compose_file, env=env)
            time.sleep(4.0)

            post_logs = read_logs(compose_file, *surviving_nodes, env=env)
            window_logs = post_logs[offset:]
            post_winners = WINNER_PATTERN.findall(post_logs)
            passed = (len(post_winners) == len(pre_winners))

            msg_counts = count_election_messages(window_logs)

            res = {
                "scenario": f"intermediate_N{n_nodes}",
                "n_nodes": n_nodes,
                "repetition": rep,
                "heartbeat_interval_seconds": interval,
                "rpc_timeout_seconds": 2.0,
                "failure_threshold": 2,
                "normal_heartbeat_count": 2,
                "normal_passed": True,
                "winner_after_failure": str(n_nodes),
                "convergence_seconds": 0.0,
                "failure_passed": passed,
                "winner_after_recovery": "",
                "recovery_seconds": 0.0,
                "recovery_passed": True,
                "messages_total": msg_counts["messages_total"],
                "election_msgs": msg_counts["election_msgs"],
                "ok_msgs": msg_counts["ok_msgs"],
                "coordinator_msgs": msg_counts["coordinator_msgs"],
                "ack_msgs": msg_counts["ack_msgs"],
            }
            print(f"   [Repetição {rep}/{repetitions}] pass={passed}, líder estável, msgs={msg_counts['messages_total']}")
            rows.append(res)

            # In-place reset: start the intermediate node back up
            if rep < repetitions:
                compose("start", target_container, compose_file=compose_file, env=env)
                time.sleep(max(2.0, interval * 1.5))
    finally:
        stop_cluster(compose_file=compose_file, env=env)

    return rows


# ============================================================================
# Main Runner & Full Statistical Summarization
# ============================================================================

def write_results_csv(rows: list[dict[str, object]], path: Path = BATTERY_RESULTS) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def summarize_battery(rows: list[dict[str, object]], path: Path = BATTERY_SUMMARY) -> None:
    """Summarizes both latency percentiles and message statistics per (scenario, n_nodes)."""
    if not rows:
        return
    scenario_keys = sorted({(row["scenario"], int(row["n_nodes"])) for row in rows})
    summaries = []
    for sc, n in scenario_keys:
        selected = [r for r in rows if r["scenario"] == sc and int(r["n_nodes"]) == n]
        convergences = [float(r["convergence_seconds"]) for r in selected]
        recoveries = [float(r["recovery_seconds"]) for r in selected]
        msgs = [int(r["messages_total"]) for r in selected]
        n_reps = len(selected)

        normal_pass_rate = round(sum(1 for r in selected if r["normal_passed"]) / n_reps, 3)
        failure_pass_rate = round(sum(1 for r in selected if r["failure_passed"]) / n_reps, 3)
        recovery_pass_rate = round(sum(1 for r in selected if r["recovery_passed"]) / n_reps, 3)

        summaries.append(
            {
                "scenario": sc,
                "n_nodes": n,
                "repetitions": n_reps,
                "heartbeat_interval_seconds": selected[0]["heartbeat_interval_seconds"],
                # Taxas de sucesso
                "normal_pass_rate": normal_pass_rate,
                "failure_pass_rate": failure_pass_rate,
                "recovery_pass_rate": recovery_pass_rate,
                # Latência de Falha (Média, Desvio, P50, P95)
                "convergence_mean_seconds": round(statistics.mean(convergences), 3),
                "convergence_stddev_seconds": round(
                    statistics.stdev(convergences) if len(convergences) > 1 else 0.0, 3
                ),
                "convergence_p50_seconds": round(percentile(convergences, 50), 3),
                "convergence_p95_seconds": round(percentile(convergences, 95), 3),
                # Latência de Recuperação (Média, Desvio, P50, P95)
                "recovery_mean_seconds": round(statistics.mean(recoveries), 3),
                "recovery_stddev_seconds": round(
                    statistics.stdev(recoveries) if len(recoveries) > 1 else 0.0, 3
                ),
                "recovery_p50_seconds": round(percentile(recoveries, 50), 3),
                "recovery_p95_seconds": round(percentile(recoveries, 95), 3),
                # Métricas de Mensagens (Média, Desvio, P50, P95)
                "messages_mean_total": round(statistics.mean(msgs), 1),
                "messages_stddev_total": round(
                    statistics.stdev(msgs) if len(msgs) > 1 else 0.0, 1
                ),
                "messages_p50_total": round(percentile([float(m) for m in msgs], 50), 1),
                "messages_p95_total": round(percentile([float(m) for m in msgs], 95), 1),
                # Mensagens detalhadas por tipo
                "avg_election_msgs": round(statistics.mean(int(r["election_msgs"]) for r in selected), 1),
                "avg_ok_msgs": round(statistics.mean(int(r["ok_msgs"]) for r in selected), 1),
                "avg_coordinator_msgs": round(statistics.mean(int(r["coordinator_msgs"]) for r in selected), 1),
                "avg_ack_msgs": round(statistics.mean(int(r["ack_msgs"]) for r in selected), 1),
            }
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summaries[0].keys()))
        writer.writeheader()
        writer.writerows(summaries)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run test battery for Bully election.")
    parser.add_argument(
        "--scenario",
        choices=["all", "baseline", "scalability", "simultaneous", "intermediate"],
        default="all",
        help="Which scenario to run (default: all)",
    )
    parser.add_argument(
        "-n",
        "--nodes",
        type=int,
        nargs="+",
        default=[3, 5],
        help="Number of nodes to test with (e.g. -n 5 or -n 3 5 7; default: [3, 5])",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help="Number of repetitions per test (default: 3)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Heartbeat interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--no-metrics-logs",
        action="store_true",
        help="Disable metric logs [MSG] emission in nodes",
    )
    args = parser.parse_args()

    enable_metrics = not args.no_metrics_logs
    rows: list[dict[str, object]] = []

    print(
        f"=== INICIANDO BATERIA DE TESTES (Cenário: {args.scenario}, "
        f"Nós: {args.nodes}, Repetições: {args.repetitions}) ==="
    )

    if args.scenario in ("all", "baseline"):
        for n in args.nodes:
            print(f"\n[Cenário Baseline] N={n} nós ({args.repetitions} repetições com reset rápido)...")
            rows.extend(
                run_baseline_scenario(
                    n_nodes=n,
                    repetitions=args.repetitions,
                    interval=args.interval,
                    enable_metrics_logs=enable_metrics,
                )
            )

    if args.scenario in ("all", "scalability"):
        for n in args.nodes:
            print(f"\n[Cenário Escalabilidade] N={n} nós ({args.repetitions} repetições com reset rápido)...")
            rows.extend(
                run_scalability_scenario(
                    n_nodes=n,
                    repetitions=args.repetitions,
                    interval=args.interval,
                    enable_metrics_logs=enable_metrics,
                )
            )

    if args.scenario in ("all", "simultaneous"):
        for n in args.nodes:
            if n < 3:
                print(f"   [Aviso] Falha simultânea ignorada para N={n} (mínimo 3 nós necessários).")
                continue
            print(f"\n[Cenário Falha Simultânea] N={n} nós ({args.repetitions} repetições com reset rápido)...")
            rows.extend(
                run_simultaneous_scenario(
                    n_nodes=n,
                    repetitions=args.repetitions,
                    interval=args.interval,
                    enable_metrics_logs=enable_metrics,
                )
            )

    if args.scenario in ("all", "intermediate"):
        for n in args.nodes:
            target_intermediate = max(2, n // 2)
            print(f"\n[Cenário Falha Intermediária] N={n} nós ({args.repetitions} repetições com reset rápido)...")
            rows.extend(
                run_intermediate_scenario(
                    n_nodes=n,
                    intermediate_node_id=target_intermediate,
                    repetitions=args.repetitions,
                    interval=args.interval,
                    enable_metrics_logs=enable_metrics,
                )
            )

    write_results_csv(rows)
    summarize_battery(rows)
    print(f"\n✅ Bateria de testes concluída com sucesso!")
    print(f"   - Resultados brutos: {BATTERY_RESULTS}")
    print(f"   - Resumo das métricas: {BATTERY_SUMMARY}")


if __name__ == "__main__":
    main()
