#!/usr/bin/env python3
"""
Interactive or automated live demonstration of the Bully election
and Heartbeat failure detection system.

Stages:
1. Starts the cluster (Docker Compose).
2. Streams live logs while the cluster stabilizes in normal state (Heartbeats / Pongs).
3. Injects a coordinator failure (stops node N).
4. Monitors Bully election in real time until a new leader converges.
5. (Optional) Restores node N to demonstrate failback/recovery.
6. Cleans up containers.
"""

import argparse
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_compose import write_dynamic_compose

# ANSI Colors
BOLD = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"

NODE_COLORS = {
    1: CYAN,
    2: GREEN,
    3: YELLOW,
    4: MAGENTA,
    5: BLUE,
}


def color_for_node(node_id: int) -> str:
    return NODE_COLORS.get(node_id, CYAN)


def banner(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'=' * 70}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'=' * 70}{RESET}\n")


class LiveLogStreamer:
    """Streams Docker Compose logs in a background thread with color highlighting."""

    def __init__(self, compose_file: str = "compose.yaml"):
        self.compose_file = compose_file
        self.proc: subprocess.Popen[str] | None = None
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.captured_logs: list[str] = []
        self.new_coordinator_event = threading.Event()
        self.new_coordinator_id: int | None = None
        self.recovery_event = threading.Event()
        self.recovered_coordinator_id: int | None = None

    def start(self) -> None:
        cmd = [
            "docker",
            "compose",
            "-f",
            self.compose_file,
            "logs",
            "-f",
            "--tail=0",
        ]
        self.proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.thread = threading.Thread(target=self._stream_loop, daemon=True)
        self.thread.start()

    def _stream_loop(self) -> None:
        assert self.proc and self.proc.stdout
        for line in iter(self.proc.stdout.readline, ""):
            if self.stop_event.is_set():
                break
            clean_line = line.strip()
            if not clean_line:
                continue

            self.captured_logs.append(clean_line)

            # Detect leader announcement: "Eleição ...: nó X tornou-se coordenador"
            m_win = re.search(r"Eleição .*: nó (\d+) tornou-se coordenador", clean_line)
            if m_win:
                self.new_coordinator_id = int(m_win.group(1))
                self.new_coordinator_event.set()

            # Format line nicely for terminal
            formatted = self._format_log_line(clean_line)
            print(formatted, flush=True)

    def reset_election_listener(self) -> None:
        self.new_coordinator_event.clear()
        self.new_coordinator_id = None

    def _format_log_line(self, line: str) -> str:
        # Match node prefix: e.g. "node1-1  | 2026-11-16 ... Ping enviado..."
        m_prefix = re.match(r"(node(\d+)-\d+)\s*\|\s*(.*)", line)
        if m_prefix:
            _, node_id_str, text = m_prefix.groups()
            nid = int(node_id_str)
            col = color_for_node(nid)
            # Highlight elections or coordinators
            if "tornou-se coordenador" in text:
                text = f"{BOLD}{GREEN}{text}{RESET}"
            elif "Iniciando eleição" in text or "StartElection" in text:
                text = f"{BOLD}{YELLOW}{text}{RESET}"
            elif "Falha consecutiva" in text or "DEADLINE" in text:
                text = f"{RED}{text}{RESET}"
            elif "[MSG]" in text:
                text = f"{DIM}{text}{RESET}"

            return f"{col}[Nó {nid}]{RESET} {text}"
        return f"{DIM}{line}{RESET}"

    def stop(self) -> None:
        self.stop_event.set()
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def run_command(cmd: list[str], timeout: int = 30) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True, text=True, timeout=timeout)


def wait_step(step_pause: bool, auto_delay: float, message: str) -> None:
    if step_pause:
        input(f"\n{BOLD}{YELLOW}👉 Pressione [ENTER] para: {message}...{RESET}")
    else:
        print(f"\n{DIM}⏳ Aguardando {auto_delay:.1f}s ({message})...{RESET}")
        time.sleep(auto_delay)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Live demonstration of Bully election and Heartbeat failure detection"
    )
    parser.add_argument(
        "-n",
        "--nodes",
        type=int,
        default=3,
        help="Number of nodes in cluster (default: 3)",
    )
    parser.add_argument(
        "-i",
        "--heartbeat-interval",
        type=float,
        default=1.0,
        help="Heartbeat interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--step",
        action="store_true",
        help="Pause and wait for [ENTER] between demonstration steps",
    )
    parser.add_argument(
        "--no-recovery",
        action="store_true",
        help="Do not demonstrate leader recovery (failback)",
    )
    parser.add_argument(
        "--auto-delay",
        type=float,
        default=3.5,
        help="Delay in seconds between steps when not using --step (default: 3.5)",
    )

    args = parser.parse_args()

    compose_file = "compose.yaml"
    if args.nodes != 3 or args.heartbeat_interval != 3.0:
        compose_file = "compose.dynamic.yaml"
        print(f"{DIM}Gerando {compose_file} para {args.nodes} nós (heartbeat={args.heartbeat_interval}s)...{RESET}")
        write_dynamic_compose(
            num_nodes=args.nodes,
            output_path=ROOT / compose_file,
            coordinator_id=args.nodes,
            rpc_timeout=2.0,
            heartbeat_interval=args.heartbeat_interval,
            failure_threshold=2,
        )

    streamer = LiveLogStreamer(compose_file=compose_file)

    try:
        # -------------------------------------------------------------
        # PASSO 1: Subir o cluster
        # -------------------------------------------------------------
        banner(f"PASSO 1: Subindo cluster com {args.nodes} nós lógicos (Líder inicial: Nó {args.nodes})")
        print(f"Executando: docker compose -f {compose_file} up -d --build")
        run_command(["docker", "compose", "-f", compose_file, "up", "-d"])

        # Iniciar captura de logs em tempo real
        streamer.start()

        # -------------------------------------------------------------
        # PASSO 2: Observar estado normal (heartbeats)
        # -------------------------------------------------------------
        banner(f"PASSO 2: Estado Normal (Monitoramento via Heartbeat gRPC)")
        print(f"Os nós seguidores 1..{args.nodes-1} monitoram o coordenador Nó {args.nodes} periodicamente:")
        wait_step(args.step, args.auto_delay + 2.0, "avançar para injeção de falha")

        # -------------------------------------------------------------
        # PASSO 3: Injetar falha no coordenador
        # -------------------------------------------------------------
        failed_leader = args.nodes
        expected_new_leader = args.nodes - 1
        banner(f"PASSO 3: Injetando Falha no Coordenador (Parando Nó {failed_leader})")
        print(f"{BOLD}{RED}Simulando falha do processo coordenador: docker compose stop node{failed_leader}{RESET}")
        streamer.reset_election_listener()
        run_command(["docker", "compose", "-f", compose_file, "stop", f"node{failed_leader}"])
        t_fail_start = time.perf_counter()

        # -------------------------------------------------------------
        # PASSO 4: Aguardar eleição Bully e convergência
        # -------------------------------------------------------------
        banner(f"PASSO 4: Detecção de Falha e Eleição Bully em Andamento")
        print(f"Aguardando seguidores detectarem a falha e elegerem o Nó {expected_new_leader}...")

        converged = streamer.new_coordinator_event.wait(timeout=12.0)
        t_converged = time.perf_counter()
        elapsed = t_converged - t_fail_start

        if converged:
            print(f"\n{BOLD}{GREEN}✅ ELEIÇÃO CONCLUÍDA COM SUCESSO!{RESET}")
            print(f"{BOLD}   - Novo Coordenador Eleito: Nó {streamer.new_coordinator_id}{RESET}")
            print(f"{BOLD}   - Tempo Total de Convergência: {elapsed:.2f} segundos{RESET}\n")
        else:
            print(f"\n{BOLD}{RED}❌ Timeout aguardando convergência da eleição!{RESET}\n")

        wait_step(args.step, args.auto_delay, "verificar estabilidade do novo líder")

        # -------------------------------------------------------------
        # PASSO 5: Recuperação / Failback (opcional)
        # -------------------------------------------------------------
        if not args.no_recovery:
            banner(f"PASSO 5: Recuperação do Nó {failed_leader} (Demonstração de Failback)")
            streamer.reset_election_listener()
            print(f"Reiniciando o líder original: docker compose start node{failed_leader}")
            t_rec_start = time.perf_counter()
            run_command(["docker", "compose", "-f", compose_file, "start", f"node{failed_leader}"])
            print(f"O Nó {failed_leader} reinicia, detecta sua maior prioridade e reassume a liderança:")
            recovered = streamer.new_coordinator_event.wait(timeout=8.0)
            elapsed_rec = time.perf_counter() - t_rec_start
            if recovered:
                print(f"\n{BOLD}{GREEN}✅ FAILBACK CONCLUÍDO COM SUCESSO!{RESET}")
                print(f"{BOLD}   - Coordenador Restaurado: Nó {streamer.new_coordinator_id}{RESET}")
                print(f"{BOLD}   - Tempo de Recuperação: {elapsed_rec:.2f} segundos{RESET}\n")
            wait_step(args.step, args.auto_delay + 1.0, "finalizar o experimento")

        # -------------------------------------------------------------
        # PASSO 6: Encerramento
        # -------------------------------------------------------------
        banner("PASSO 6: Finalizando o Cluster e Limpando Recursos")
        print(f"Executando: docker compose -f {compose_file} down -t 1")

    finally:
        streamer.stop()
        subprocess.run(
            ["docker", "compose", "-f", compose_file, "down", "--remove-orphans", "-t", "1"],
            cwd=ROOT,
            check=False,
            capture_output=True,
        )
        if compose_file == "compose.dynamic.yaml" and (ROOT / "compose.dynamic.yaml").exists():
            (ROOT / "compose.dynamic.yaml").unlink()
        banner("DEMONSTRAÇÃO CONCLUÍDA")


if __name__ == "__main__":
    main()
