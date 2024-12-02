# Nodes for Account A
ACCOUNT_A_NODES = {
    'node2A': {'ip': 'localhost', 'port': 5002},
    'node2A_replica1': {'ip': 'localhost', 'port': 5004},
    'node2A_replica2': {'ip': 'localhost', 'port': 5005},
}

# Nodes for Account B
ACCOUNT_B_NODES = {
    'node3B': {'ip': 'localhost', 'port': 5003},
    'node3B_replica1': {'ip': 'localhost', 'port': 5006},
    'node3B_replica2': {'ip': 'localhost', 'port': 5007},
}

# Coordinator Node
COORDINATOR_NODE = {
    'node1': {'ip': 'localhost', 'port': 5001},
}

# Combined Nodes
NODES = {**COORDINATOR_NODE, **ACCOUNT_A_NODES, **ACCOUNT_B_NODES}


# Timeout settings (in seconds)
ELECTION_TIMEOUT = (1.0, 2.0)  # Adjusted for faster testing
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats