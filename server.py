import os
import threading
import time
from concurrent import futures
from typing import Optional

import grpc

import node_pb2
import node_pb2_grpc


def read_endpoints(value: str) -> dict[int, str]:
    return {
        int(item.split("=", 1)[0]): item.split("=", 1)[1]
        for item in value.split(",")
        if item.strip()
    }


class NodeState:
    def __init__(
        self,
        node_id: int,
        coordinator_id: int,
        endpoints: dict[int, str],
        rpc_timeout: float,
        heartbeat_interval: float,
        failure_threshold: int,
    ) -> None:
        self.node_id = node_id
        self.coordinator_id: Optional[int] = coordinator_id
        self.endpoints = endpoints
        self.current_election_id = 0
        self.election_active = False
        self.rpc_timeout = rpc_timeout
        self.heartbeat_interval = heartbeat_interval
        self.failure_threshold = failure_threshold
        self.lock = threading.Lock()


def run_election(state: NodeState, election_id: int) -> None:
    higher_nodes = sorted(
        node_id
        for node_id in state.endpoints
        if node_id > state.node_id
    )
    higher_node_responded = False

    for node_id in higher_nodes:
        try:
            with grpc.insecure_channel(state.endpoints[node_id]) as channel:
                stub = node_pb2_grpc.NodeStub(channel)
                response = stub.StartElection(
                    node_pb2.ElectionMsg(
                        sender_id=state.node_id,
                        election_id=election_id,
                    ),
                    timeout=state.rpc_timeout,
                )
            if response.accepted:
                higher_node_responded = True
                print(
                    f"Eleição {election_id}: nó {node_id} respondeu OK",
                    flush=True,
                )
        except grpc.RpcError:
            print(
                f"Eleição {election_id}: nó {node_id} não respondeu",
                flush=True,
            )

    with state.lock:
        if state.current_election_id != election_id:
            return
        if higher_node_responded:
            state.election_active = False
            return

        state.coordinator_id = state.node_id
        state.election_active = False

    print(
        f"Eleição {election_id}: nó {state.node_id} tornou-se coordenador",
        flush=True,
    )
    announce_coordinator(state, election_id)


def start_election(
    state: NodeState,
    election_id: int | None = None,
    originated_by: int | None = None,
) -> None:
    with state.lock:
        if election_id is None:
            election_id = max(state.current_election_id + 1, time.time_ns())
        if election_id < state.current_election_id:
            return
        if election_id == state.current_election_id and state.election_active:
            return
        state.current_election_id = election_id
        state.coordinator_id = None
        state.election_active = True

    if originated_by is not None:
        print(
            f"Nó {state.node_id} assumiu a eleição {election_id} do nó {originated_by}",
            flush=True,
        )
    else:
        print(
            f"Nó {state.node_id} iniciou a eleição {election_id}",
            flush=True,
        )
    threading.Thread(
        target=run_election,
        args=(state, election_id),
        daemon=True,
    ).start()


def announce_coordinator(state: NodeState, election_id: int) -> None:
    for node_id, endpoint in state.endpoints.items():
        if node_id == state.node_id:
            continue
        try:
            with grpc.insecure_channel(endpoint) as channel:
                stub = node_pb2_grpc.NodeStub(channel)
                stub.AnnounceCoordinator(
                    node_pb2.CoordinatorMsg(
                        coordinator_id=state.node_id,
                        sender_id=state.node_id,
                        election_id=election_id,
                    ),
                    timeout=state.rpc_timeout,
                )
        except grpc.RpcError:
            print(
                f"Anúncio da eleição {election_id} não chegou ao nó {node_id}",
                flush=True,
            )


class NodeService(node_pb2_grpc.NodeServicer):
    def __init__(self, state: NodeState) -> None:
        self.state = state

    def Heartbeat(self, request, context):
        return node_pb2.Pong(
            responder_id=self.state.node_id,
            election_id=request.election_id,
            alive=True,
        )

    def StartElection(self, request, context):
        should_start_election = False
        with self.state.lock:
            is_new_election = request.election_id > self.state.current_election_id
            if is_new_election:
                self.state.current_election_id = request.election_id
                self.state.coordinator_id = None
                self.state.election_active = False
            should_start_election = (
                request.sender_id < self.state.node_id
                and not self.state.election_active
            )

        if should_start_election:
            start_election(
                self.state,
                request.election_id,
                originated_by=request.sender_id,
            )

        return node_pb2.OkMsg(
            responder_id=self.state.node_id,
            election_id=request.election_id,
            accepted=request.sender_id < self.state.node_id,
        )

    def AnnounceCoordinator(self, request, context):
        accepted = False
        with self.state.lock:
            if request.election_id >= self.state.current_election_id:
                self.state.current_election_id = request.election_id
                self.state.coordinator_id = request.coordinator_id
                self.state.election_active = False
                accepted = True

        if accepted:
            print(
                f"Novo coordenador reconhecido: Nó {request.coordinator_id} "
                f"(eleição {request.election_id})",
                flush=True,
            )

        return node_pb2.AckMsg(
            responder_id=self.state.node_id,
            election_id=request.election_id,
            accepted=accepted,
        )


def heartbeat_loop(state: NodeState) -> None:
    consecutive_failures = 0

    while True:
        with state.lock:
            coordinator_id = state.coordinator_id

        if coordinator_id is None or coordinator_id == state.node_id:
            time.sleep(state.heartbeat_interval)
            continue

        try:
            target = state.endpoints[coordinator_id]
            with grpc.insecure_channel(target) as channel:
                stub = node_pb2_grpc.NodeStub(channel)
                response = stub.Heartbeat(
                    node_pb2.Ping(
                        sender_id=state.node_id,
                        election_id=state.current_election_id,
                    ),
                    timeout=state.rpc_timeout,
                )
            if not response.alive or response.responder_id != coordinator_id:
                raise RuntimeError("resposta de heartbeat inválida")
            print(
                f"Ping enviado para Coordenador {coordinator_id}; "
                f"Pong recebido do nó {response.responder_id}",
                flush=True,
            )
            consecutive_failures = 0
        except grpc.RpcError:
            consecutive_failures += 1
            print(
                f"ALERTA: falha {consecutive_failures}/"
                f"{state.failure_threshold} ao contactar "
                f"o Coordenador {coordinator_id}",
                flush=True,
            )
            if consecutive_failures >= state.failure_threshold:
                print("Iniciando eleição", flush=True)
                start_election(state)
                consecutive_failures = 0
        except (KeyError, RuntimeError) as error:
            print(f"Falha no heartbeat: {error}", flush=True)
            consecutive_failures += 1
            if consecutive_failures >= state.failure_threshold:
                print("Iniciando eleição", flush=True)
                start_election(state)
                consecutive_failures = 0
        time.sleep(state.heartbeat_interval)


def main() -> None:
    node_id = int(os.environ["NODE_ID"])
    port = int(os.getenv("NODE_PORT", "50051"))
    coordinator_id = int(os.getenv("COORDINATOR_ID", "3"))
    rpc_timeout = float(os.getenv("RPC_TIMEOUT_SECONDS", "2"))
    heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "3"))
    failure_threshold = int(os.getenv("HEARTBEAT_FAILURE_THRESHOLD", "2"))
    endpoints = read_endpoints(os.environ["NODE_ENDPOINTS"])
    state = NodeState(
        node_id,
        coordinator_id,
        endpoints,
        rpc_timeout,
        heartbeat_interval,
        failure_threshold,
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node_pb2_grpc.add_NodeServicer_to_server(NodeService(state), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Nó {node_id} escutando na porta {port}", flush=True)

    start_election(state)

    threading.Thread(
        target=heartbeat_loop,
        args=(state,),
        daemon=True,
    ).start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=0)


if __name__ == "__main__":
    main()