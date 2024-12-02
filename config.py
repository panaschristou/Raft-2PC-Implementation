# Description: This file contains the configuration settings for the simulation.
from enum import Enum

NODES = {
    'node1': {'ip': 'localhost', 'port': 5001},  # Node-1
    'node2': {'ip': 'localhost', 'port': 5002},    # Node-2
    'node3': {'ip': 'localhost', 'port': 5003}     # Node-3
}

class SimulationScenario(Enum):
    CRASH_BEFORE_PREPARE = 1
    CRASH_BEFORE_COMMIT = 2
    COORDINATOR_CRASH_BEFORE_COMMIT = 3
    COORDINATOR_DIFFERENT_PREPARE_COMMIT_LOG = 4
    COORDINATOR_RECOVERS_AFTER_PREPARE = 5

# Timeout settings (in seconds)
ELECTION_TIMEOUT = (1.0, 2.0)  # Adjusted for faster testing
HEARTBEAT_INTERVAL = 0.5  # Interval for leader to send heartbeats