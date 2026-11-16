#!/usr/bin/env python3
"""
Interactive live demonstration of the Bully election and Heartbeat failure detection system.

Runs exactly ONE chosen experiment with real-time log streaming and formatted metrics:
1. Baseline: Leader failure followed by leader recovery (failback).
2. Scalability: Simple leader failure (node N fails -> node N-1 assumes).
3. Simultaneous: Concurrent failure of the top two nodes (N and N-1 fail -> N-2 assumes).
4. Intermediate: Failure of a subordinate node (intermediate fails -> leader remains stable).
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
    print(f"\n{BOLD}{CYAN}{'=' * 75}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'=' * 75}{RESET}\n")


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
        self.spurious_election_detected = False

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

            if "Iniciando eleição" in clean_line:
                self.spurious_election_detected = True

            # Format line nicely for terminal
            formatted = self._format_log_line(clean_line)
            print(formatted, flush=True)

    def _format_log_line(self, line: str) -> str:
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
            elif "Falha consecutiva" in text or "ALERTA" in text or "DEADLINE" in text:
                text = f"{RED}{text}{RESET}"
            elif "[MSG]" in text:
                text = f"{DIM}{text}{RESET}"

            return f"{col}[Nó {nid}]{RESET} {text}"
        return f"{DIM}{line}{RESET}"

    def reset_election_listener(self) -> None:
        self.new_coordinator_event.clear()
        self.new_coordinator_id = None
        self.spurious_election_detected = False

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


def interactive_selection() -> tuple[str, int, float]:
    """Presents a friendly interactive menu to select scenario, cluster size, and heartbeat interval."""
    print(f"{BOLD}{CYAN}==========================================================================={RESET}")
    print(f"{BOLD}{CYAN}       DEMONSTRAÇÃO DE EXPERIMENTOS MC714 (Bully + Heartbeat)             {RESET}")
    print(f"{BOLD}{CYAN}==========================================================================={RESET}\n")
    print(f"{BOLD}Escolha o experimento que deseja executar:{RESET}")
    print(f"  {BOLD}[1]{RESET} {GREEN}Baseline:{RESET} Falha do Líder + Recuperação (Nó N cai -> N-1 assume -> N volta)")
    print(f"  {BOLD}[2]{RESET} {CYAN}Escalabilidade:{RESET} Falha Simples do Líder (Nó N cai -> N-1 assume)")
    print(f"  {BOLD}[3]{RESET} {YELLOW}Falhas Simultâneas:{RESET} Queda Concorrente dos 2 Maiores (Nós N e N-1 caem -> N-2 assume)")
    print(f"  {BOLD}[4]{RESET} {MAGENTA}Falha Intermediária:{RESET} Queda de Nó Subordinado (Nó intermediário cai -> Líder estável)\n")

    sc_map = {"1": "baseline", "2": "scalability", "3": "simultaneous", "4": "intermediate"}

    choice = ""
    while choice not in sc_map:
        raw = input(f"{BOLD}Digite o número do experimento [1-4] (padrão: 1): {RESET}").strip()
        if not raw:
            choice = "1"
            break
        if raw in sc_map:
            choice = raw
        else:
            print(f"{RED}Opção inválida. Digite 1, 2, 3 ou 4.{RESET}")

    scenario = sc_map[choice]

    nodes = 3
    while True:
        raw_nodes = input(f"{BOLD}Digite o número de nós lógicos [3, 5, 7, 10] (padrão: 3): {RESET}").strip()
        if not raw_nodes:
            nodes = 3
            break
        if raw_nodes.isdigit() and int(raw_nodes) >= 3:
            nodes = int(raw_nodes)
            break
        print(f"{RED}Número de nós deve ser um inteiro maior ou igual a 3.{RESET}")

    hb_interval = 2.0
    while True:
        raw_hb = input(f"{BOLD}Digite o intervalo do heartbeat em segundos [Ex: 1.0, 2.0, 3.0] (padrão: 2.0): {RESET}").strip()
        if not raw_hb:
            hb_interval = 2.0
            break
        try:
            val = float(raw_hb.replace(",", "."))
            if val > 0:
                hb_interval = val
                break
            print(f"{RED}O intervalo deve ser maior que zero.{RESET}")
        except ValueError:
            print(f"{RED}Entrada inválida. Digite um valor numérico positivo (ex: 2.0).{RESET}")

    return scenario, nodes, hb_interval


def run_experiment_demo(
    scenario: str,
    nodes: int,
    heartbeat_interval: float = 2.0,
    step_pause: bool = False,
    auto_delay: float = 3.5,
) -> None:
    """Executes exactly ONE chosen experiment scenario with live log streaming."""

    compose_file = "compose.dynamic.yaml"
    print(f"{DIM}Gerando {compose_file} ({nodes} nós, heartbeat={heartbeat_interval:.1f}s)...{RESET}")
    write_dynamic_compose(
        num_nodes=nodes,
        output_path=ROOT / compose_file,
        coordinator_id=nodes,
        rpc_timeout=2.0,
        heartbeat_interval=heartbeat_interval,
        failure_threshold=2,
    )

    streamer = LiveLogStreamer(compose_file=compose_file)

    try:
        # -------------------------------------------------------------
        # ETAPA 1: Inicialização do Cluster
        # -------------------------------------------------------------
        banner(f"ETAPA 1: Subindo cluster com {nodes} nós (Líder inicial: Nó {nodes})")
        print(f"Cenário Selecionado: {BOLD}{scenario.upper()}{RESET} | Nós: {BOLD}{nodes}{RESET} | Heartbeat: {BOLD}{heartbeat_interval:.1f}s{RESET}")
        print(f"Executando: docker compose -f {compose_file} up -d")
        run_command(["docker", "compose", "-f", compose_file, "up", "-d"])

        streamer.start()

        # -------------------------------------------------------------
        # ETAPA 2: Estabilização e Heartbeats Normais
        # -------------------------------------------------------------
        banner(f"ETAPA 2: Estado Normal (Monitoramento via Heartbeat gRPC)")
        print(f"Os nós subordinados 1..{nodes-1} monitoram periodicamente o líder Nó {nodes} a cada {heartbeat_interval:.1f}s:")
        wait_step(step_pause, max(auto_delay + 1.5, heartbeat_interval * 2.0), "avançar para injeção de falha")

        # -------------------------------------------------------------
        # ETAPA 3 & 4: Injeção de Falha e Convergência por Cenário
        # -------------------------------------------------------------
        if scenario in ("baseline", "scalability"):
            failed_leader = nodes
            expected_new_leader = nodes - 1
            banner(f"ETAPA 3: Injetando Falha no Coordenador (Parando Nó {failed_leader})")
            print(f"{BOLD}{RED}Simulando falha do processo coordenador: docker compose stop node{failed_leader}{RESET}")
            streamer.reset_election_listener()
            run_command(["docker", "compose", "-f", compose_file, "stop", f"node{failed_leader}"])
            t_fail_start = time.perf_counter()

            banner(f"ETAPA 4: Detecção de Falha e Eleição Bully em Andamento")
            print(f"Aguardando seguidores detectarem a falha e elegerem o Nó {expected_new_leader}...")

            timeout_election = max(15.0, (heartbeat_interval * 2.5) + 12.0)
            converged = streamer.new_coordinator_event.wait(timeout=timeout_election)
            elapsed = time.perf_counter() - t_fail_start

            if converged and streamer.new_coordinator_id == expected_new_leader:
                print(f"\n{BOLD}{GREEN}✅ ELEIÇÃO CONCLUÍDA COM SUCESSO!{RESET}")
                print(f"{BOLD}   - Novo Coordenador Eleito: Nó {streamer.new_coordinator_id}{RESET}")
                print(f"{BOLD}   - Tempo Total de Convergência: {elapsed:.2f} segundos{RESET}\n")
            else:
                print(f"\n{BOLD}{RED}❌ Falha na convergência ou líder inesperado ({streamer.new_coordinator_id})!{RESET}\n")

            wait_step(step_pause, auto_delay, "verificar estabilidade")

            # Se for baseline, demonstrar também a recuperação / failback:
            if scenario == "baseline":
                banner(f"ETAPA 5: Recuperação do Nó {failed_leader} (Demonstração de Failback)")
                streamer.reset_election_listener()
                print(f"Reiniciando o líder original: docker compose start node{failed_leader}")
                t_rec_start = time.perf_counter()
                run_command(["docker", "compose", "-f", compose_file, "start", f"node{failed_leader}"])
                print(f"O Nó {failed_leader} reinicia, detecta sua maior prioridade e reassume a liderança:")
                timeout_rec = max(10.0, (heartbeat_interval * 2.0) + 8.0)
                recovered = streamer.new_coordinator_event.wait(timeout=timeout_rec)
                elapsed_rec = time.perf_counter() - t_rec_start
                if recovered and streamer.new_coordinator_id == failed_leader:
                    print(f"\n{BOLD}{GREEN}✅ FAILBACK CONCLUÍDO COM SUCESSO!{RESET}")
                    print(f"{BOLD}   - Coordenador Restaurado: Nó {streamer.new_coordinator_id}{RESET}")
                    print(f"{BOLD}   - Tempo de Recuperação: {elapsed_rec:.2f} segundos{RESET}\n")
                wait_step(step_pause, auto_delay + 1.0, "finalizar o experimento")

        elif scenario == "simultaneous":
            f1, f2 = nodes, nodes - 1
            expected_new_leader = nodes - 2
            banner(f"ETAPA 3: Falha Simultânea Concorrente dos Nós {f1} e {f2}")
            print(f"{BOLD}{RED}Derrubando simultaneamente os dois maiores nós: docker compose stop node{f1} node{f2}{RESET}")
            streamer.reset_election_listener()
            run_command(["docker", "compose", "-f", compose_file, "stop", f"node{f1}", f"node{f2}"])
            t_fail_start = time.perf_counter()

            banner(f"ETAPA 4: Eleição Bully sob Falha Múltipla Concorrente")
            print(f"Aguardando o Nó {expected_new_leader} assumir a liderança sem impasses...")

            timeout_election = max(18.0, (heartbeat_interval * 2.5) + 14.0)
            converged = streamer.new_coordinator_event.wait(timeout=timeout_election)
            elapsed = time.perf_counter() - t_fail_start

            if converged and streamer.new_coordinator_id == expected_new_leader:
                print(f"\n{BOLD}{GREEN}✅ ELEIÇÃO EM FALHA CONCORRENTE CONCLUÍDA COM SUCESSO!{RESET}")
                print(f"{BOLD}   - Novo Coordenador Eleito: Nó {streamer.new_coordinator_id}{RESET}")
                print(f"{BOLD}   - Tempo de Convergência: {elapsed:.2f} segundos{RESET}\n")
            else:
                print(f"\n{BOLD}{RED}❌ Falha na convergência em falha múltipla!{RESET}\n")

            wait_step(step_pause, auto_delay + 1.0, "finalizar o experimento")

        elif scenario == "intermediate":
            interm_node = max(2, nodes // 2)
            banner(f"ETAPA 3: Falha de Nó Intermediário Subordinado (Nó {interm_node})")
            print(f"{BOLD}{YELLOW}Parando o nó subordinado {interm_node}: docker compose stop node{interm_node}{RESET}")
            print(f"O coordenador principal (Nó {nodes}) continua em execução e saudável.")
            streamer.reset_election_listener()
            run_command(["docker", "compose", "-f", compose_file, "stop", f"node{interm_node}"])

            banner(f"ETAPA 4: Verificação de Estabilidade do Líder")
            wait_time = max(5.0, heartbeat_interval * 2.5)
            print(f"Monitorando cluster por {wait_time:.1f} segundos para confirmar que nenhuma eleição espúria ocorre...")
            time.sleep(wait_time)

            if not streamer.spurious_election_detected:
                print(f"\n{BOLD}{GREEN}✅ ESTABILIDADE COMPROVADA COM SUCESSO!{RESET}")
                print(f"{BOLD}   - Coordenador Principal (Nó {nodes}) permaneceu estável.{RESET}")
                print(f"{BOLD}   - Zero eleições indevidas disparadas pelos nós restantes.{RESET}\n")
            else:
                print(f"\n{BOLD}{RED}⚠️ Eleição indevida detectada!{RESET}\n")

            wait_step(step_pause, auto_delay, "finalizar o experimento")

        # -------------------------------------------------------------
        # ETAPA FINAL: Limpeza
        # -------------------------------------------------------------
        banner("ETAPA FINAL: Limpeza do Cluster")
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Live demonstration of Bully election and Heartbeat failure detection"
    )
    parser.add_argument(
        "-s",
        "--scenario",
        choices=["baseline", "scalability", "simultaneous", "intermediate", "1", "2", "3", "4"],
        default=None,
        help="Experiment scenario to demonstrate (if omitted, shows interactive menu)",
    )
    parser.add_argument(
        "-n",
        "--nodes",
        type=int,
        default=None,
        help="Number of nodes in cluster (default: 3)",
    )
    parser.add_argument(
        "-i",
        "--heartbeat-interval",
        type=float,
        default=None,
        help="Heartbeat interval in seconds (default: 2.0, prompted if omitted in interactive mode)",
    )
    parser.add_argument(
        "--step",
        action="store_true",
        help="Pause and wait for [ENTER] between demonstration steps",
    )
    parser.add_argument(
        "--auto-delay",
        type=float,
        default=3.5,
        help="Delay in seconds between steps when not using --step (default: 3.5)",
    )

    args = parser.parse_args()

    sc_alias = {"1": "baseline", "2": "scalability", "3": "simultaneous", "4": "intermediate"}

    scenario = args.scenario
    if scenario in sc_alias:
        scenario = sc_alias[scenario]

    nodes = args.nodes
    hb_interval = args.heartbeat_interval

    # If scenario or nodes were omitted, prompt the interactive menu
    if scenario is None or nodes is None:
        menu_scenario, menu_nodes, menu_hb = interactive_selection()
        if scenario is None:
            scenario = menu_scenario
        if nodes is None:
            nodes = menu_nodes
        if hb_interval is None:
            hb_interval = menu_hb
    elif hb_interval is None:
        hb_interval = 2.0

    run_experiment_demo(
        scenario=scenario,
        nodes=nodes,
        heartbeat_interval=hb_interval,
        step_pause=args.step,
        auto_delay=args.auto_delay,
    )


if __name__ == "__main__":
    main()
