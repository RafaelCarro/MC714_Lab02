import os
import threading
import time
from concurrent import futures

import grpc

import node_pb2
import node_pb2_grpc


def read_endpoints(value: str) -> dict[int, str]:
    return {
        int(item.split("=", 1)[0]): item.split("=", 1)[1]
        for item in value.split(",")
        if item.strip()
    }


class NodeService(node_pb2_grpc.NodeServicer):
    def __init__(self, node_id: int) -> None:
        self.node_id = node_id

    def Heartbeat(self, request, context):
        return node_pb2.Pong(
            responder_id=self.node_id,
            election_id=request.election_id,
            alive=True,
        )

    def StartElection(self, request, context):
        return node_pb2.OkMsg(
            responder_id=self.node_id,
            election_id=request.election_id,
            accepted=request.sender_id < self.node_id,
        )

    def AnnounceCoordinator(self, request, context):
        return node_pb2.AckMsg(
            responder_id=self.node_id,
            election_id=request.election_id,
            accepted=True,
        )


def heartbeat_loop(
    node_id: int, coordinator_id: int, endpoints: dict[int, str]
) -> None:
    if node_id == coordinator_id:
        return

    target = endpoints[coordinator_id]
    while True:
        try:
            with grpc.insecure_channel(target) as channel:
                stub = node_pb2_grpc.NodeStub(channel)
                stub.Heartbeat(
                    node_pb2.Ping(sender_id=node_id, election_id=0),
                    timeout=2,
                )
            print("Ping enviado para Coordenador...", flush=True)
        except grpc.RpcError:
            print(
                "ALERTA: Coordenador caiu. Deveria chamar a eleição aqui",
                flush=True,
            )
        time.sleep(3)


def main() -> None:
    node_id = int(os.environ["NODE_ID"])
    port = int(os.getenv("NODE_PORT", "50051"))
    coordinator_id = int(os.getenv("COORDINATOR_ID", "3"))
    endpoints = read_endpoints(os.environ["NODE_ENDPOINTS"])

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node_pb2_grpc.add_NodeServicer_to_server(NodeService(node_id), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Nó {node_id} escutando na porta {port}", flush=True)

    threading.Thread(
        target=heartbeat_loop,
        args=(node_id, coordinator_id, endpoints),
        daemon=True,
    ).start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=0)


if __name__ == "__main__":
    main()