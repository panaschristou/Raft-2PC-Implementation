from node_2pc import TwoPhaseCommitNode
from node import Node
from config import NODES
import sys

class CoordinatorNode(TwoPhaseCommitNode):
    def __init__(self, name):
        super().__init__(name, "Coordinator")

    def start_2pc(self, participants, transactions):
    # Phase 1: Prepare
        prepared_nodes = []
        for participant, delta in transactions.items():
            response = self.send_rpc(NODES[participant]['ip'], NODES[participant]['port'], '2pc_prepare', {'delta': delta})
            if response and response['status'] == 'prepared':
                prepared_nodes.append(participant)
            else:
                # Abort if any participant cannot prepare
                phase = 'abort'
                for p in prepared_nodes:
                    self.send_rpc(NODES[p]['ip'], NODES[p]['port'], f'2pc_{phase}', {'delta': transactions[p]})
                return False  # Transaction aborted

        # Phase 2: Commit
        phase = 'commit'
        for participant, delta in transactions.items():
            response = self.send_rpc(NODES[participant]['ip'], NODES[participant]['port'], f'2pc_{phase}', {'delta': delta})
            if not response or response['status'] != 'committed':
                # Handle failure during commit phase
                print(f"Failed to commit on participant {participant}.")
                return False  # Transaction aborted
        return True  # Transaction committed


if __name__ == '__main__':
    """
    Main entry point for the Coordinator node application.
    """
    if len(sys.argv) != 2:
        print("Usage: python coordinator.py [node_name]")
        sys.exit(1)
    # Validate node name from config file
    node_name = sys.argv[1]
    if node_name != 'node1':
        print("Invalid coordinator node name. The coordinator must be 'node1'.")
        sys.exit(1)
    # Create and start the node
    node = CoordinatorNode(node_name)
    try:
        node.start()
    except KeyboardInterrupt:
        print(f"[{node.name}] Shutting down...")
    finally:
        node.running = False
        if node.server_socket:  # Clean up network resources
            node.server_socket.close()