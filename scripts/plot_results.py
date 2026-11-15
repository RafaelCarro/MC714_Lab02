#!/usr/bin/env python3
"""Generate publication-ready plots from experiment CSV results."""

import csv
from pathlib import Path
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
SUMMARY_CSV = ROOT / "experiments" / "summary.csv"
OUTPUT_DIR = ROOT / "experiments"


def load_summary() -> list[dict[str, float]]:
    if not SUMMARY_CSV.exists():
        raise FileNotFoundError(f"{SUMMARY_CSV} not found. Run run_experiment.py first.")

    with SUMMARY_CSV.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [{k: float(v) for k, v in row.items()} for row in reader]


def plot_convergence_vs_recovery(data: list[dict[str, float]]) -> Path:
    intervals = [d["heartbeat_interval_seconds"] for d in data]
    fail_means = [d["convergence_mean_seconds"] for d in data]
    fail_errors = [d["convergence_stddev_seconds"] for d in data]
    rec_means = [d["recovery_mean_seconds"] for d in data]
    rec_errors = [d["recovery_stddev_seconds"] for d in data]

    x = range(len(intervals))
    width = 0.35

    fig, ax = plt.subplots(figsize=(7, 4.5), dpi=300)

    rects1 = ax.bar(
        [i - width / 2 for i in x],
        fail_means,
        width,
        yerr=fail_errors,
        capsize=5,
        label="Convergência na Falha (Nó 2 assume)",
        color="#1f77b4",
        edgecolor="black",
        alpha=0.9,
    )
    rects2 = ax.bar(
        [i + width / 2 for i in x],
        rec_means,
        width,
        yerr=rec_errors,
        capsize=5,
        label="Recuperação / Failback (Nó 3 reassume)",
        color="#2ca02c",
        edgecolor="black",
        alpha=0.9,
    )

    ax.set_xlabel("Intervalo do Heartbeat (segundos)", fontsize=11, fontweight="bold")
    ax.set_ylabel("Tempo de Convergência (segundos)", fontsize=11, fontweight="bold")
    ax.set_title(
        "Tempo de Convergência na Falha vs. Recuperação do Líder",
        fontsize=12,
        fontweight="bold",
        pad=12,
    )
    ax.set_xticks(list(x))
    ax.set_xticklabels([f"{int(i)}s" for i in intervals])
    ax.legend(frameon=True, facecolor="white", edgecolor="none")
    ax.grid(axis="y", linestyle="--", alpha=0.6)

    # Add numeric labels above bars
    for rect in rects1:
        height = rect.get_height()
        ax.annotate(
            f"{height:.2f}s",
            xy=(rect.get_x() + rect.get_width() / 2, height),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    for rect in rects2:
        height = rect.get_height()
        ax.annotate(
            f"{height:.2f}s",
            xy=(rect.get_x() + rect.get_width() / 2, height),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.tight_layout()
    output_path = OUTPUT_DIR / "convergence_comparison.png"
    plt.savefig(output_path)
    plt.close()
    return output_path


def plot_percentiles(data: list[dict[str, float]]) -> Path:
    intervals = [d["heartbeat_interval_seconds"] for d in data]
    means = [d["convergence_mean_seconds"] for d in data]
    p50 = [d["convergence_p50_seconds"] for d in data]
    p95 = [d["convergence_p95_seconds"] for d in data]

    fig, ax = plt.subplots(figsize=(7, 4.5), dpi=300)

    ax.plot(
        intervals,
        means,
        marker="o",
        linewidth=2,
        color="#1f77b4",
        label="Média",
    )
    ax.plot(
        intervals,
        p50,
        marker="s",
        linestyle="--",
        linewidth=2,
        color="#ff7f0e",
        label="Mediana (p50)",
    )
    ax.plot(
        intervals,
        p95,
        marker="^",
        linestyle="-.",
        linewidth=2,
        color="#d62728",
        label="Percentil 95 (p95)",
    )

    ax.set_xlabel("Intervalo do Heartbeat (segundos)", fontsize=11, fontweight="bold")
    ax.set_ylabel("Latência de Convergência (segundos)", fontsize=11, fontweight="bold")
    ax.set_title(
        "Percentis de Latência de Convergência após Falha do Líder",
        fontsize=12,
        fontweight="bold",
        pad=12,
    )
    ax.set_xticks(intervals)
    ax.set_xticklabels([f"{int(i)}s" for i in intervals])
    ax.legend(frameon=True, facecolor="white", edgecolor="none")
    ax.grid(True, linestyle="--", alpha=0.6)

    plt.tight_layout()
    output_path = OUTPUT_DIR / "latency_percentiles.png"
    plt.savefig(output_path)
    plt.close()
    return output_path


def main() -> None:
    data = load_summary()
    p1 = plot_convergence_vs_recovery(data)
    p2 = plot_percentiles(data)
    print(f"Gráficos gerados com sucesso:\n - {p1}\n - {p2}")


if __name__ == "__main__":
    main()
