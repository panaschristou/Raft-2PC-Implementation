NODES = {
    'coordinator': {'ip': 'localhost', 'port': 5001},  # Node-1
    'account_a': {'ip': 'localhost', 'port': 5002},    # Node-2
    'account_b': {'ip': 'localhost', 'port': 5003}     # Node-3
}

# Transaction timeout settings
PREPARE_TIMEOUT = 10.0
COMMIT_TIMEOUT = 10.0