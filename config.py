NODES = {
    'coordinator': {'ip': 'localhost', 'port': 5001},  # Node-1
    'account_a': {'ip': 'localhost', 'port': 5002},    # Node-2
    'account_b': {'ip': 'localhost', 'port': 5003}     # Node-3
}

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (1.0, 2.0)  # Adjusted for faster testing
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats