from node_2pc import TwoPhaseCommitNode
import sys
from config import NODES, CLUSTER_A_NODES, CLUSTER_B_NODES

class ParticipantNode(TwoPhaseCommitNode):
    def __init__(self, name):
        # Initialize as a participant with its name
        role = "Participant"
        super().__init__(name, role)
        print(f"[{self.name}] Initialized as a participant node in cluster {self.cluster_name}")

    def handle_2pc_prepare(self, data):
        """
        Override to ensure only RAFT leader handles 2PC operations
        """
        if self.state != 'Leader':
            print(f"[{self.name}] Received 2PC prepare but not cluster leader. Current state: {self.state}")
            return {'status': 'error', 'message': 'Not the cluster leader'}
        return super().handle_2pc_prepare(data)

    def handle_2pc_commit(self, data):
        """
        Override to ensure only RAFT leader handles 2PC operations
        """
        if self.state != 'Leader':
            print(f"[{self.name}] Received 2PC commit but not cluster leader. Current state: {self.state}")
            return {'status': 'error', 'message': 'Not the cluster leader'}
        return super().handle_2pc_commit(data)

    def replicate_state_change(self, change_type, data):
        """
        Replicates state changes to other nodes in the RAFT cluster
        """
        if self.state == 'Leader':
            self.replicate_to_cluster(change_type, data)

if __name__ == '__main__':
    """
    Main entry point for the Participant node application.
    """
    if len(sys.argv) != 2:
        print("Usage: python participant.py [node_name]")
        sys.exit(1)

    # Validate node name from config file
    node_name = sys.argv[1]
    if not (node_name in CLUSTER_A_NODES or node_name in CLUSTER_B_NODES):
        print(f"Invalid node name. Must be part of CLUSTER_A_NODES or CLUSTER_B_NODES")
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
