#!/usr/bin/env python3
"""Run repeated normal/failure experiments and write raw and summary CSVs."""

import csv
import os
import re
import statistics
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "experiments" / "results.csv"
SUMMARY = ROOT / "experiments" / "summary.csv"
WINNER_PATTERN = re.compile(r"Eleição .*: nó (\d+) tornou-se coordenador")
HEARTBEAT_PATTERN = re.compile(r"Pong recebido do nó 3")


def compose(*args: str, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        capture_output=True,
        timeout=30,
    )
    return result.stdout


def stop_cluster(env: dict[str, str] | None = None) -> None:
    subprocess.run(
        ["docker", "compose", "down", "--remove-orphans", "-t", "1"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        timeout=15,
    )


def read_logs(env: dict[str, str]) -> str:
    return compose(
        "logs",
        "--no-log-prefix",
        "--no-color",
        "--tail=300",
        "node1",
        "node2",
        env=env,
    )


def run_configuration(interval: float, repetition: int) -> dict[str, object]:
    env = os.environ.copy()
    env.update(
        {
            "HEARTBEAT_INTERVAL_SECONDS": str(interval),
            "RPC_TIMEOUT_SECONDS": "2",
            "HEARTBEAT_FAILURE_THRESHOLD": "2",
        }
    )
    compose("up", "-d", env=env)
    startup_wait = max(4.0, interval * 2 + 1)
    time.sleep(startup_wait)

    normal_logs = read_logs(env)
    normal_heartbeats = len(HEARTBEAT_PATTERN.findall(normal_logs))
    normal_passed = normal_heartbeats >= 2

    failure_injected_at = time.monotonic()
    compose("stop", "-t", "1", "node3", env=env)
    deadline = failure_injected_at + 20
    logs = ""
    winner = ""
    while time.monotonic() < deadline:
        logs = read_logs(env)
        match = WINNER_PATTERN.search(logs)
        if match:
            winner = match.group(1)
            break
        time.sleep(0.5)

    return {
        "repetition": repetition,
        "heartbeat_interval_seconds": interval,
        "rpc_timeout_seconds": 2,
        "failure_threshold": 2,
        "normal_heartbeat_count": normal_heartbeats,
        "normal_passed": normal_passed,
        "winner_after_node3_failure": winner,
        "convergence_seconds": round(
            time.monotonic() - failure_injected_at, 3
        ),
        "failure_passed": winner == "2",
    }


def percentile(values: list[float], percentage: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentage / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def summarize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    summaries = []
    intervals = sorted({row["heartbeat_interval_seconds"] for row in rows})
    for interval in intervals:
        selected = [
            row for row in rows
            if row["heartbeat_interval_seconds"] == interval
        ]
        convergence = [float(row["convergence_seconds"]) for row in selected]
        summaries.append(
            {
                "heartbeat_interval_seconds": interval,
                "repetitions": len(selected),
                "normal_pass_rate": round(
                    sum(row["normal_passed"] for row in selected)
                    / len(selected),
                    3,
                ),
                "failure_pass_rate": round(
                    sum(row["failure_passed"] for row in selected)
                    / len(selected),
                    3,
                ),
                "convergence_mean_seconds": round(
                    statistics.mean(convergence), 3
                ),
                "convergence_stddev_seconds": round(
                    statistics.stdev(convergence) if len(convergence) > 1 else 0,
                    3,
                ),
                "convergence_p50_seconds": round(
                    percentile(convergence, 50), 3
                ),
                "convergence_p95_seconds": round(
                    percentile(convergence, 95), 3
                ),
            }
        )
    return summaries


def main() -> None:
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    compose("build")
    rows = []
    repetitions = int(os.getenv("EXPERIMENT_REPETITIONS", "3"))
    try:
        for interval in (1, 2, 3):
            for repetition in range(1, repetitions + 1):
                stop_cluster()
                rows.append(run_configuration(interval, repetition))
    finally:
        stop_cluster()

    with RESULTS.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    summaries = summarize(rows)
    with SUMMARY.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=summaries[0].keys())
        writer.writeheader()
        writer.writerows(summaries)

    for row in rows:
        print(row)
    for summary in summaries:
        print(summary)


if __name__ == "__main__":
    main()
