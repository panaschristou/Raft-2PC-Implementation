from enum import Enum

COORDINATOR_NODE = {
    'node1': {'ip': 'localhost', 'port': 5001},
}

# Cluster A (Account A) - Raft cluster
CLUSTER_A_NODES = {
    'nodeA1': {'ip': 'localhost', 'port': 5002},  # Raft node1
    'nodeA2': {'ip': 'localhost', 'port': 5004},  # Raft node2
    'nodeA3': {'ip': 'localhost', 'port': 5005},  # Raft node3
}

# Cluster B (Account B) - Raft cluster
CLUSTER_B_NODES = {
    'nodeB1': {'ip': 'localhost', 'port': 5003},  # Raft node1
    'nodeB2': {'ip': 'localhost', 'port': 5006},  # Raft node2
    'nodeB3': {'ip': 'localhost', 'port': 5007},  # Raft node3
}

# Combined node configuration
NODES = {**COORDINATOR_NODE, **CLUSTER_A_NODES, **CLUSTER_B_NODES}

class SimulationScenario(Enum):
    CRASH_BEFORE_PREPARE = '1'
    CRASH_BEFORE_COMMIT = '2'
    COORDINATOR_CRASH_AFTER_SENDING_PREPARE = '3'
    COORDINATOR_CRASH_AFTER_SENDING_COMMIT= '4'

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (1.0, 2.0)  # Adjusted for faster testing
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats
