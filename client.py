import socket
import json
import sys
from config import NODES, CLUSTER_A_NODES, CLUSTER_B_NODES

class BaseClient:
    """
    Base client class for interacting with the Raft cluster.
    """

    @staticmethod
    def send_rpc(ip, port, rpc_type, data):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((ip, port))
                message = json.dumps({'rpc_type': rpc_type, 'data': data})
                s.sendall(message.encode())
                response = s.recv(4096).decode()
                return json.loads(response)
        except Exception as e:
            print(f"RPC Error: {e}")
            return None

    def submit_value(self, value):
        """Submit a value to the Raft cluster, discovering the leader automatically."""
        for node_name in NODES:
            node_info = NODES[node_name]
            print(f"Attempting to submit value through node {node_name}")

            response = self.send_rpc(node_info['ip'], node_info['port'], 'SubmitValue', {'value': value})

            if not response:
                print(f"Node {node_name} is unreachable")
                continue

            if response.get('success'):
                print(f"Value successfully committed to the cluster")
                return True

            if response.get('redirect') and response.get('leader_name'):
                leader_name = response['leader_name']
                if leader_name in NODES:
                    leader_info = NODES[leader_name]
                    print(f"Redirecting to leader {leader_name}")
                    response = self.send_rpc(leader_info['ip'], leader_info['port'], 'SubmitValue', {'value': value})
                    if response and response.get('success'):
                        print(f"Value successfully committed to the cluster")
                        return True

        print("Failed to submit value to the cluster - no leader available")
        return False

    def print_all_logs(self):
        """Print logs from all nodes in the cluster."""
        print("\nRequesting logs from all nodes...")
        for node_name in NODES:
            node_info = NODES[node_name]
            print(f"\nContacting node {node_name}...")
            response = self.send_rpc(node_info['ip'], node_info['port'], 'PrintLog', {})
            if response:
                print(f"Successfully got log from {node_name}")
            else:
                print(f"Failed to get log from {node_name}")

    def trigger_leader_change(self):
            """Now tries all nodes in both clusters."""
            for nodes in [CLUSTER_A_NODES, CLUSTER_B_NODES]:
                for node_name, node_info in nodes.items():
                    response = self.send_rpc(
                        node_info['ip'],
                        node_info['port'],
                        'TriggerLeaderChange',
                        {}
                    )
                    if response and response.get('status') == 'Leader stepping down':
                        print(f"Leader {node_name} is stepping down.")
                        return

    def simulate_crash(self, node_name):
        """Updated to handle new cluster structure."""
        cluster_nodes = {**CLUSTER_A_NODES, **CLUSTER_B_NODES}
        if node_name not in cluster_nodes:
            print(f"Invalid node name: {node_name}")
            return

        node_info = cluster_nodes[node_name]
        response = self.send_rpc(
            node_info['ip'],
            node_info['port'],
            'SimulateCrash',
            {}
        )
        if response and response.get('status') == 'Node crashed':
            print(f"Node {node_name} has simulated a crash")
        else:
            print(f"Failed to crash node {node_name}")

if __name__ == '__main__':
    client = BaseClient()
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py submit [value]")
        print("  python client.py leader_change")
        print("  python client.py simulate_crash")
        sys.exit(1)

    command = sys.argv[1]

    if command == 'submit':
        if len(sys.argv) != 3:
            print("Usage: python client.py submit [value]")
            sys.exit(1)
        value = sys.argv[2]
        client.submit_value(value)
    elif command == 'leader_change':
        client.trigger_leader_change()
    elif command == 'simulate_crash':
        node_name = sys.argv[2]
        client.simulate_crash(node_name)
    elif command == 'print_logs':
        client.print_all_logs()
    else:
        print("Unknown command.")
