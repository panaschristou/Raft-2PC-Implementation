import socket
import json
from node import Node

class TwoPhaseCommitNode(Node):
    def __init__(self, name, role, cluster_nodes=None):
        super().__init__(name, cluster_nodes)
        self.role = role  # Coordinator or Participant
        self.account_balance = 0
        self.account_file = f"{self.name}_account.txt"
        if role == "Participant":
            self.load_account_balance()

    def load_account_balance(self):
        try:
            with open(self.account_file, 'r') as f:
                self.account_balance = int(f.read().strip())
        except FileNotFoundError:
            with open(self.account_file, 'w') as f:
                f.write("0")
            self.account_balance = 0

    def save_account_balance(self):
        with open(self.account_file, 'w') as f:
            f.write(str(self.account_balance))
    
    def get_account_balance(self):
        return self.account_balance

    def prepare_transaction(self, delta):
        if self.account_balance + delta < 0:
            return False
        return True
    
    def check_transaction_status(self):
        # JUST SOMETHING TO HAVE IT DEFINED
        return {'status': 'committed' if self.account_balance >= 0 else 'aborted'}

    def commit_transaction(self, delta):
        self.account_balance += delta
        self.save_account_balance()

    def handle_2pc_prepare(self, data):
        delta = data.get('delta', 0)
        if self.prepare_transaction(delta):
            return {'status': 'prepared'}
        return {'status': 'abort'}

    def handle_2pc_commit(self, data):
        delta = data.get('delta', 0)
        # Use Raft consensus to commit the transaction
        success = self.submit_value(delta)
        if success:
            # The transaction has been committed via Raft consensus
            # Update the account balance
            self.commit_transaction(delta)
            return {'status': 'committed'}
        else:
            # Failed to achieve consensus; abort the transaction
            return {'status': 'abort'}


    def handle_2pc_request(self, data):
        if data['phase'] == 'prepare':
            return self.handle_2pc_prepare(data)
        elif data['phase'] == 'commit':
            return self.handle_2pc_commit(data)
        return {'status': 'error', 'message': 'Unknown phase'}

    def handle_client_connection(self, client_socket: socket.socket):
        """
        Mananges incoming client connections and RPC requests. It takes a client socket as input.
        Processes different types of RPCs and returns appropriate responses.
        """
        try:
            
            data = client_socket.recv(4096).decode() # Receive and decode client request
            if data:
                request = json.loads(data) # Parse JSON request
                rpc_type = request['rpc_type']
                response = {}

                # Manange different RPC types with thread safety
                with self.lock:
                    if rpc_type == 'RequestVote':
                        # Handle voting requests during leader election
                        response = self.handle_request_vote(request['data'])
                    elif rpc_type == 'AppendEntries':
                        # Handle log replication and heartbeat messages
                        response = self.handle_append_entries(request['data'])
                    elif rpc_type == 'SubmitValue':
                        # Handle client value submissions
                        response = self.handle_client_submit(request['data'])
                    elif rpc_type == 'TriggerLeaderChange':
                        # Handle manual leader step-down requests
                        response = self.trigger_leader_change()
                    elif rpc_type == 'SimulateCrash':
                        # Handle crash simulation requests
                        response = self.simulate_crash()
                    elif rpc_type == 'PrintLog':
                        # Handle print log request
                        response = self.print_node_log()
                    elif rpc_type in ['2pc_prepare', '2pc_commit']:
                        # Delegate to 2PC-specific handler
                        response = self.handle_2pc_request(request['data'])    
                    elif rpc_type == 'GetBalance':
                        # Handle client balance requests
                        # MAYBE THIS IS NOT CORRECT
                        response = self.get_account_balance()
                    elif rpc_type == 'CheckTransactionStatus':
                        # Handle transaction status check requests
                        response = self.check_transaction_status()
                    else:
                        response = {'error': 'Unknown RPC type'}

                # Send response back to client
                client_socket.sendall(json.dumps(response).encode())
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close()  # Ensure socket is closed even if an error occurs
            
            



