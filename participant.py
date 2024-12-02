from node_2pc import TwoPhaseCommitNode
import sys
from config import NODES, ACCOUNT_A_NODES, ACCOUNT_B_NODES

class ParticipantNode(TwoPhaseCommitNode):
    def __init__(self, name):
        # Determine cluster nodes
        if name in ACCOUNT_A_NODES:
            cluster_nodes = ACCOUNT_A_NODES
        elif name in ACCOUNT_B_NODES:
            cluster_nodes = ACCOUNT_B_NODES
        else:
            print("Invalid participant node name.")
            sys.exit(1)
        # Initialize as a participant with its name and cluster nodes
        super().__init__(name, role="Participant", cluster_nodes=cluster_nodes)
        print(f"[{self.name}] Initialized as a participant node.")

    # Any additional participant-specific methods can be added here
    def run(self):
        """
        Start the participant node's operations, such as listening for coordinator's RPCs.
        """
        print(f"[{self.name}] Participant node is running.")
        self.start()  # Calls the `start` method from the parent Node class

if __name__ == '__main__':
    """
    Main entry point for the Participant node application.
    """
    if len(sys.argv) != 2:
        print("Usage: python participant.py [node_name]")
        sys.exit(1)
    node_name = sys.argv[1]
    if node_name not in NODES:
        print(f"Invalid node name. Available nodes: {list(NODES.keys())}")
        sys.exit(1)
    # Create and start the node
    node = ParticipantNode(node_name)
    try:
        node.start()
    except KeyboardInterrupt:
        print(f"[{node.name}] Shutting down...")
    finally:
        node.running = False
        if node.server_socket:  # Clean up network resources
            node.server_socket.close()