from node import Node  # Note: Changed from node_2pc import to avoid RAFT behavior
from config import COORDINATOR_NODE, CLUSTER_A_NODES, CLUSTER_B_NODES
import socket
import json
import sys
import threading
import time
from config import SimulationScenario

class CoordinatorNode:
    def __init__(self, name):
        self.name = name
        self.ip = COORDINATOR_NODE[name]['ip']
        self.port = COORDINATOR_NODE[name]['port']
        self.running = True
        self.server_socket = None
        self.timeout_duration = 2.0
        self.cluster_leaders = {
            'A': None,
            'B': None
        }

    def start(self):
        """Initialize and start the coordinator's server."""
        print(f"[{self.name}] Starting coordinator...")
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        print(f"[{self.name}] Server listening at {self.ip}:{self.port}")

        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()
                client_thread = threading.Thread(target=self.handle_client_connection, args=(client_socket,))
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                if self.running:
                    print(f"[{self.name}] Server error: {e}")

    def handle_client_connection(self, client_socket):
        """Manages incoming client connections and RPC requests."""
        try:
            data = client_socket.recv(4096).decode()
            if data:
                request = json.loads(data)
                rpc_type = request['rpc_type']
                response = {}

                if rpc_type == 'GetLeaderStatus':
                    response = {'is_leader': self.state == 'Leader'}
                elif rpc_type == '2pc_request':
                    response = {'status': 'committed' if self.start_2pc(request['data']) else 'aborted'}
                elif rpc_type == 'SetBalance':
                    response = self.handle_set_account_balance(request['data'])
                elif rpc_type == 'PrintAllLogs':
                    response = self.print_all_logs()           
                else:
                    response = {'error': 'Unknown RPC type'}

                client_socket.sendall(json.dumps(response).encode())
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close()

    def send_rpc(self, ip, port, rpc_type, data, timeout=2.0):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((ip, port))
                message = json.dumps({'rpc_type': rpc_type, 'data': data})
                s.sendall(message.encode())
                response = s.recv(4096).decode()
                return json.loads(response)
        except Exception as e:
            print(f"RPC Error: {e}")
            return None
        
    def handle_set_account_balance(self, data ):
        """Set account balance from coordinator"""
        account = data.get('account')
        balance = data.get('balance')
        cluster_letter = account[-1] if account.startswith('Account') else account
        data = {'balance': int(balance)}
        
        # Find current RAFT leader for the cluster
        leader = self.find_cluster_leader(cluster_letter)
        if not leader:
            print(f"No leader found for cluster {cluster_letter}")
            return False
        
        # Send the transaction to the leader
        node_info = self.get_node_info(leader)
        response = self.send_rpc(
            node_info['ip'],
            node_info['port'],
            'SetBalance',
            data
        )
        return response

    def start_2pc(self, data):
        """
        Start 2PC protocol with account-level transactions
        Args:
            transactions: Dict with format {'AccountA': delta_a, 'AccountB': delta_b}
        """
        # Convert account-level transactions to current leader-level transactions
        leader_transactions = {}
        transactions = data['transactions']
        simulation_num = data['simulation_num']
        
        # Find current RAFT leaders for each account cluster
        for cluster_id, delta in transactions.items():
            cluster_letter = cluster_id[-1]  # 'A' or 'B' from 'AccountA' or 'AccountB'
            
            # Find the current leader in the cluster
            leader = self.find_cluster_leader(cluster_letter)
            if not leader:
                print(f"No leader found for cluster {cluster_letter}")
                return False
                
            # Send only relevant transaction to each leader
            leader_transactions[leader] = {
                'transactions': {cluster_id: delta},
                'simulation_num': simulation_num
            }

        # Phase 1: Prepare
        prepared = True
        prepare_responses = {}
        
        for leader, tx in leader_transactions.items():
            node_info = self.get_node_info(leader)
            
            #--------------- RPC with timeout ---------------
            start_time = time.time()
            response = None
            while time.time() - start_time < self.timeout_duration:
                response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_prepare', tx)
                if response:
                    break
                time.sleep(0.1)  # Avoid busy-waiting
            #------------------------------------------------
            
            # response = self.send_rpc(
            #     node_info['ip'],
            #     node_info['port'],
            #     '2pc_prepare',
            #     tx
            # )
            
            if not response:
                print(f"No response from leader {leader} during prepare phase")
                prepared = False
                break
                
            if response.get('status') != 'prepared':
                print(f"Leader {leader} not prepared")
                prepared = False
                break
                
            prepare_responses[leader] = response
        
        if simulation_num == SimulationScenario.COORDINATOR_CRASH_AFTER_SENDING_PREPARE.value:
            print(f"[{self.name}] Simulating coordinator crash after sending prepare requests")
            self.simulate_crash_sleep()
            print(f'Resending prepare requests to leaders: {leader_transactions.keys()}')
            for leader, tx in leader_transactions.items():
                node_info = self.get_node_info(leader)
                response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_prepare', tx)
                if not response or response.get('status') != 'prepared':
                    print(f"Leader {leader} not prepared")
                    prepared = False
                    break
                else:
                    prepared = True
                    print('Resend successful.')

        # Phase 2: Commit
        if prepared:
            for leader, tx in leader_transactions.items():
                node_info = self.get_node_info(leader)
                
                #--------------- RPC with timeout ---------------
                start_time = time.time()
                response = None
                while time.time() - start_time < self.timeout_duration:
                    response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_commit', tx)
                    if response:
                        break
                    time.sleep(0.1)  # Avoid busy-waiting
                #------------------------------------------------
                
                # response = self.send_rpc(
                #     node_info['ip'],
                #     node_info['port'],
                #     '2pc_commit',
                #     tx
                # )
                
                if simulation_num == SimulationScenario.COORDINATOR_CRASH_AFTER_SENDING_COMMIT.value:
                    print(f"[{self.name}] Simulating coordinator crash after sending commit requests")
                    self.simulate_crash_sleep()
                    print('Getting all logs to compare commit and prepare last entries.')
                    all_logs = self.get_all_logs()
                    for node_name, logs in all_logs.items():
                        for leader, tx in leader_transactions.items():
                            if node_name == leader:
                                last_prepare_transaction_id = logs[0].get('transaction_id')
                                last_commit_transaction_id = logs[1].get('transaction_id')
                                if last_prepare_transaction_id != last_commit_transaction_id:
                                    print(f"Last prepare and commit transaction IDs do not match for {node_name}")
                                    return {'status': 'abort', 'message': 'Cluster did not agree to commit while coordinator crashed.'}
                    print('All logs match. Transaction committed while coordinator was down.')
                    return {'status': 'committed'}
                            
                
                if not response:
                    print(f"No response from leader {leader} during commit phase")
                    break
            
                if response.get('status') != 'committed':
                    return False

        return {'status': 'committed'}

    def find_cluster_leader(self, cluster_letter):
        """
        Find current RAFT leader in specified cluster
        Returns: leader node name or None if no leader found
        """
        cluster_nodes = CLUSTER_A_NODES if cluster_letter == 'A' else CLUSTER_B_NODES
        for node_name, node_info in cluster_nodes.items():
            response = self.send_rpc(
                node_info['ip'],
                node_info['port'],
                'GetLeaderStatus',
                {}
            )
            if response and response.get('is_leader', False):
                return node_name
        return None

    def get_node_info(self, node_name):
        """Get node connection information"""
        if node_name.startswith('nodeA'):
            return CLUSTER_A_NODES[node_name]
        elif node_name.startswith('nodeB'):
            return CLUSTER_B_NODES[node_name]
        return None
    
    def get_all_logs(self):
        """Retrieve logs from all nodes in the system"""
        all_logs = {}
        for node_name, node_info in {**CLUSTER_A_NODES, **CLUSTER_B_NODES}.items():
            response = self.send_rpc(
                node_info['ip'],
                node_info['port'],
                'GetLogs',
                {}
            )
            if response:
                all_logs[node_name] = response.get('all_logs', [])
        return all_logs
    
    def print_all_logs(self):
        """Print logs from all nodes in the system"""
        all_logs = self.get_all_logs()
        for node_name, logs in all_logs.items():
            print(f"\nLogs from node {node_name}:")
            for entry in logs:
                print(entry)
        return {'status': 'success'}
    
    def simulate_crash_sleep(self):
        """
        Simulates a crash by:
        1. Temporarily disconnects from network to allow new leader election
        """
        self.simulating_crash_ongoing = True
        
        # Close current socket
        if self.server_socket:
            self.server_socket.close()
            self.server_socket = None
        
        print(f"[{self.name}] Going to sleep for 10 seconds to allow new leader election...")
        time.sleep(10)
        
        # Create and bind new socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        
        self.simulating_crash_ongoing = False
        print("Coordinator rejoining cluster")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python coordinator.py [node_name]")
        sys.exit(1)

    node_name = sys.argv[1]
    if node_name not in COORDINATOR_NODE:
        print("Invalid coordinator node name.")
        sys.exit(1)

    node = CoordinatorNode(node_name)
    try:
        node.start()
    except KeyboardInterrupt:
        print(f"[{node.name}] Shutting down...")
    finally:
        node.running = False
        if node.server_socket:
            node.server_socket.close()
