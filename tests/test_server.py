from unittest.mock import Mock

import node_pb2

from server import NodeService, NodeState, read_endpoints
from scripts.run_experiment import percentile


ENDPOINTS = {1: "node1:50051", 2: "node2:50051", 3: "node3:50051"}


def make_state(node_id: int = 2) -> NodeState:
    return NodeState(
        node_id=node_id,
        coordinator_id=3,
        endpoints=ENDPOINTS,
        rpc_timeout=0.1,
        heartbeat_interval=0.1,
        failure_threshold=2,
    )


def test_read_endpoints_parses_node_addresses() -> None:
    assert read_endpoints(
        "1=node1:50051,2=node2:50051,3=node3:50051"
    ) == ENDPOINTS


def test_heartbeat_returns_alive_pong() -> None:
    state = make_state(node_id=3)
    service = NodeService(state)

    response = service.Heartbeat(
        node_pb2.Ping(sender_id=1, election_id=42),
        Mock(),
    )

    assert response.responder_id == 3
    assert response.election_id == 42
    assert response.alive is True


def test_higher_node_accepts_election_from_lower_node() -> None:
    state = make_state(node_id=3)
    state.current_election_id = 42
    state.election_active = True
    service = NodeService(state)

    response = service.StartElection(
        node_pb2.ElectionMsg(sender_id=1, election_id=42),
        Mock(),
    )

    assert response.responder_id == 3
    assert response.election_id == 42
    assert response.accepted is True


def test_lower_node_rejects_election_from_higher_node() -> None:
    state = make_state(node_id=1)
    service = NodeService(state)

    response = service.StartElection(
        node_pb2.ElectionMsg(sender_id=2, election_id=42),
        Mock(),
    )

    assert response.responder_id == 1
    assert response.accepted is False


def test_stale_coordinator_announcement_is_rejected() -> None:
    state = make_state(node_id=1)
    state.current_election_id = 100
    service = NodeService(state)

    response = service.AnnounceCoordinator(
        node_pb2.CoordinatorMsg(
            coordinator_id=2,
            sender_id=2,
            election_id=99,
        ),
        Mock(),
    )

    assert response.accepted is False
    assert state.coordinator_id == 3


def test_new_coordinator_announcement_is_applied() -> None:
    state = make_state(node_id=1)
    service = NodeService(state)

    response = service.AnnounceCoordinator(
        node_pb2.CoordinatorMsg(
            coordinator_id=2,
            sender_id=2,
            election_id=101,
        ),
        Mock(),
    )

    assert response.accepted is True
    assert state.coordinator_id == 2
    assert state.current_election_id == 101


def test_percentile_interpolates_between_measurements() -> None:
    assert percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.5
    assert percentile([1.0, 2.0, 3.0, 4.0], 95) == 3.85
