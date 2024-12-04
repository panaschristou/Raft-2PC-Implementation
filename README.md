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
## System Architecture

- Coordinator Node: Manages 2PC protocol
- Cluster A: Handles Account A transactions
- Cluster B: Handles Account B transactions
- Each cluster implements Raft consensus internally

## Crash Simulation
The system supports various crash scenarios:

1. Crash before prepare
2. Crash before commit
3. Coordinator crash after sending prepare
4. Coordinator crash after sending commit

## Created Files (persistent files)

- `node*_lab2Raft.txt` - Persistent Raft logs
- `node*_account.txt` - Account balance storage
- `node*_prepare_log.json` - 2PC prepare phase logs
- `node*_commit_log.json` - 2PC commit phase logs

## Error Handling 🧨
Our implementation can handle the following errors:
- Network failures
- Node crashes
- Transaction failures
- Leader election timeouts
