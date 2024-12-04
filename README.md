# Distributed Transaction System with Raft Consensus

A distributed transaction system implementing both Raft consensus algorithm and Two-Phase Commit (2PC) protocol for maintaining consistency across distributed nodes.

## Features

- **Raft Consensus Implementation**
  - Leader Election
  - Log Replication
  - Log Persistence
  - Cluster Management
  - Crash Recovery

- **Two-Phase Commit (2PC)**
  - Atomic Transactions
  - Account Balance Management
  - Transaction Logging
  - Crash Recovery Support

- **Multi-Cluster Architecture**
  - Separate Raft Clusters (A and B)
  - Coordinator Node Management
  - Cross-Cluster Transactions

## Components

- `coordinator.py` - Implements the 2PC coordinator
- `node.py` - Base Raft node implementation
- `node_2pc.py` - Extended node with 2PC support
- `client.py` - Base client implementation
- `client_2pc.py` - Extended client with 2PC operations
- `participant.py` - 2PC participant node
- `config.py` - System configuration

## Setup

1. Ensure Python 3.x is installed
2. Clone the repository
3. Set up the configuration in `config.py`

## Usage

### Update config file according to your setup

```

```

### Starting Nodes

```sh
# Start coordinator
python coordinator.py node1

# Start cluster A nodes
python participant.py nodeA1
python participant.py nodeA2
python participant.py nodeA3

# Start cluster B nodes
python participant.py nodeB1
python participant.py nodeB2
python participant.py nodeB3
```

### Client Operations
```sh
# Perform transaction
python client_2pc.py transaction <value1: AccountA> <value2: AccountB> [bonus] [simulation_num]

# Check balances
python client_2pc.py get_balances

# Set balance
python client_2pc.py set_balance <account_name [AccountA/AccountB]> <balance>

# Print logs
python client_2pc.py print_logs
```
