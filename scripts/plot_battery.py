#!/usr/bin/env python3
"""Generates publication-ready plots for the test battery metrics."""

import csv
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
BATTERY_SUMMARY = ROOT / "experiments" / "battery_summary.csv"
BATTERY_RESULTS = ROOT / "experiments" / "battery_results.csv"
OUTPUT_DIR = ROOT / "experiments"


def load_summary() -> list[dict[str, object]]:
    if not BATTERY_SUMMARY.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {BATTERY_SUMMARY}")
    rows = []
    with BATTERY_SUMMARY.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def plot_message_scaling(rows: list[dict[str, object]]) -> Path:
    """Plots message complexity O(N^2) scaling and breakdown by message type."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5), dpi=300)

    # --- Subplot 1: Total Messages vs N across Scenarios ---
    scenarios = {
        "scalability": ("Escalabilidade (Falha Líder)", "#1f77b4", "o-"),
        "simultaneous": ("Falha Simultânea (2 maiores)", "#d62728", "s-"),
        "baseline": ("Baseline (Falha + Recuperação)", "#2ca02c", "^--"),
    }

    for sc_prefix, (label, color, marker) in scenarios.items():
        matching = [r for r in rows if r["scenario"].startswith(sc_prefix)]
        if not matching:
            continue
        matching = sorted(matching, key=lambda x: int(x["n_nodes"]))
        n_vals = [int(r["n_nodes"]) for r in matching]
        msg_means = [float(r["messages_mean_total"]) for r in matching]
        msg_stds = [float(r["messages_stddev_total"]) for r in matching]

        ax1.errorbar(
            n_vals,
            msg_means,
            yerr=msg_stds,
            fmt=marker,
            color=color,
            label=label,
            capsize=4,
            linewidth=2,
            markersize=7,
        )

    ax1.set_title("Total de Mensagens de Eleição vs. Tamanho do Cluster ($N$)", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Número de Nós ($N$)", fontsize=11)
    ax1.set_ylabel("Mensagens Trocadas (Total)", fontsize=11)
    ax1.grid(True, linestyle="--", alpha=0.6)
    ax1.legend(loc="upper left", framealpha=0.9)
    ax1.set_xticks([3, 5, 7, 10])

    # --- Subplot 2: Message Type Breakdown (Stacked Bar for Scalability) ---
    scal_rows = sorted(
        [r for r in rows if r["scenario"].startswith("scalability")],
        key=lambda x: int(x["n_nodes"]),
    )

    if scal_rows:
        n_labels = [f"N={r['n_nodes']}" for r in scal_rows]
        x_indices = np.arange(len(n_labels))
        width = 0.55

        election_msgs = [float(r["avg_election_msgs"]) for r in scal_rows]
        ok_msgs = [float(r["avg_ok_msgs"]) for r in scal_rows]
        coord_msgs = [float(r["avg_coordinator_msgs"]) for r in scal_rows]
        ack_msgs = [float(r["avg_ack_msgs"]) for r in scal_rows]

        p1 = ax2.bar(x_indices, election_msgs, width, label="ElectionMsg (Convocação)", color="#ff7f0e")
        p2 = ax2.bar(x_indices, ok_msgs, width, bottom=election_msgs, label="OkMsg (Assunção)", color="#17becf")
        bottom_coord = np.array(election_msgs) + np.array(ok_msgs)
        p3 = ax2.bar(x_indices, coord_msgs, width, bottom=bottom_coord, label="CoordinatorMsg (Anúncio)", color="#2ca02c")
        bottom_ack = bottom_coord + np.array(coord_msgs)
        p4 = ax2.bar(x_indices, ack_msgs, width, bottom=bottom_ack, label="AckMsg (Confirmação)", color="#9467bd")

        ax2.set_title("Composição das Mensagens por Tipo (Cenário Escalabilidade)", fontsize=12, fontweight="bold")
        ax2.set_xlabel("Configuração do Cluster", fontsize=11)
        ax2.set_ylabel("Média de Mensagens Trocadas", fontsize=11)
        ax2.set_xticks(x_indices)
        ax2.set_xticklabels(n_labels)
        ax2.grid(True, axis="y", linestyle="--", alpha=0.6)
        ax2.legend(loc="upper left", framealpha=0.9)

    plt.tight_layout()
    output_path = OUTPUT_DIR / "battery_message_scaling.png"
    plt.savefig(output_path, dpi=300)
    plt.close()
    return output_path


def plot_convergence_scaling(rows: list[dict[str, object]]) -> Path:
    """Plots failure convergence latency and recovery failback latency across N."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5), dpi=300)

    # --- Subplot 1: Convergence Time vs N ---
    scenarios = {
        "scalability": ("Falha Simples (Líder N)", "#1f77b4", "o-"),
        "simultaneous": ("Falha Simultânea (N e N-1)", "#d62728", "s-"),
    }

    for sc_prefix, (label, color, marker) in scenarios.items():
        matching = [r for r in rows if r["scenario"].startswith(sc_prefix)]
        if not matching:
            continue
        matching = sorted(matching, key=lambda x: int(x["n_nodes"]))
        n_vals = [int(r["n_nodes"]) for r in matching]
        conv_means = [float(r["convergence_mean_seconds"]) for r in matching]
        conv_stds = [float(r["convergence_stddev_seconds"]) for r in matching]

        ax1.errorbar(
            n_vals,
            conv_means,
            yerr=conv_stds,
            fmt=marker,
            color=color,
            label=label,
            capsize=4,
            linewidth=2,
            markersize=7,
        )

    ax1.set_title("Tempo de Convergência vs. Número de Nós ($N$)", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Número de Nós ($N$)", fontsize=11)
    ax1.set_ylabel("Tempo de Convergência (s)", fontsize=11)
    ax1.grid(True, linestyle="--", alpha=0.6)
    ax1.legend(loc="best", framealpha=0.9)
    ax1.set_xticks([3, 5, 7, 10])

    # --- Subplot 2: Recovery (Failback) Latency ---
    baseline_rows = sorted(
        [r for r in rows if r["scenario"].startswith("baseline")],
        key=lambda x: int(x["n_nodes"]),
    )

    if baseline_rows:
        n_vals = [int(r["n_nodes"]) for r in baseline_rows]
        rec_means = [float(r["recovery_mean_seconds"]) for r in baseline_rows]
        rec_stds = [float(r["recovery_stddev_seconds"]) for r in baseline_rows]
        rec_p50 = [float(r["recovery_p50_seconds"]) for r in baseline_rows]
        rec_p95 = [float(r["recovery_p95_seconds"]) for r in baseline_rows]

        x_indices = np.arange(len(n_vals))
        width = 0.35

        ax2.bar(
            x_indices - width/2,
            rec_means,
            width,
            yerr=rec_stds,
            capsize=4,
            label="Média ± Desvio Padrão",
            color="#2ca02c",
            alpha=0.85,
        )
        ax2.bar(
            x_indices + width/2,
            rec_p95,
            width,
            label="Percentil P95",
            color="#98df8a",
            alpha=0.85,
        )

        ax2.axhline(1.0, color="gray", linestyle=":", label="Limiar 1.0s (Sub-segundo)")
        ax2.set_title("Latência de Recuperação (Failback Ativo do Maior Nó)", fontsize=12, fontweight="bold")
        ax2.set_xlabel("Tamanho do Cluster ($N$)", fontsize=11)
        ax2.set_ylabel("Tempo de Reeleição (s)", fontsize=11)
        ax2.set_xticks(x_indices)
        ax2.set_xticklabels([f"N={n}" for n in n_vals])
        ax2.grid(True, axis="y", linestyle="--", alpha=0.6)
        ax2.legend(loc="upper right", framealpha=0.9)

    plt.tight_layout()
    output_path = OUTPUT_DIR / "battery_convergence_scaling.png"
    plt.savefig(output_path, dpi=300)
    plt.close()
    return output_path


def main() -> None:
    print(f"Lendo dados de resumo da bateria: {BATTERY_SUMMARY}")
    rows = load_summary()
    p1 = plot_message_scaling(rows)
    print(f"✅ Gráfico de mensagens gerado: {p1}")
    p2 = plot_convergence_scaling(rows)
    print(f"✅ Gráfico de convergência gerado: {p2}")


if __name__ == "__main__":
    main()
