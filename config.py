NODES = {
    'node1': {'ip': 'localhost', 'port': 5001},  # Node-1
    'node2': {'ip': 'localhost', 'port': 5002},    # Node-2
    'node3': {'ip': 'localhost', 'port': 5003}     # Node-3
}

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (1.0, 2.0)  # Adjusted for faster testing
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats