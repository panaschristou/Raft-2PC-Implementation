# node_2pc.py
import socket
import json
from node import Node
from config import NODES, CLUSTER_A_NODES, CLUSTER_B_NODES
import time
from config import SimulationScenario
class TwoPhaseCommitNode(Node):
    def __init__(self, name, role):
        super().__init__(name)
        self.role = role  # Coordinator or Participant
        self.account_balance = 0
        self.transaction_id = 0
        self.transaction_status = None
        self.timeout_duration = 2
        self.prepare_log = []
        self.commit_log = []
        self.account_file = f"{self.name}_account.txt"
        self.prepare_log_file = f"{self.name}_prepare_log.json"
        self.commit_log_file = f"{self.name}_commit_log.json"
        self.cluster_name = self._determine_cluster()
        self.cluster_nodes = self._get_cluster_nodes()

        if role == "Participant":
            self.load_account_balance()
        self.load_prepare_log()
        
    # ------------------- Cluster Utilities -------------------

    def _determine_cluster(self):
        """Determines which RAFT cluster this node belongs to."""
        if self.name.startswith('nodeA'):
            return 'A'
        elif self.name.startswith('nodeB'):
            return 'B'
        return None

    def _get_cluster_nodes(self):
        """Returns the nodes in this node's RAFT cluster."""
        if self.cluster_name == 'A':
            return CLUSTER_A_NODES
        elif self.cluster_name == 'B':
            return CLUSTER_B_NODES
        return {}

    # ------------------- Log Management -------------------

    def load_prepare_log(self):
        """Loads the prepare log from file or initializes it."""
        self.prepare_log = self._load_or_initialize_json(self.prepare_log_file, [])
        if self.prepare_log:
            self.transaction_id = self.prepare_log[-1]['transaction_id'] + 1

    def save_prepare_log(self):
        """Saves the latest prepare log entry."""
        self._append_to_json_file(self.prepare_log_file, self.prepare_log[-1])

    def load_commit_log(self):
        """Loads the commit log from file or initializes it."""
        self.commit_log = self._load_or_initialize_json(self.commit_log_file, [])

    def save_commit_log(self):
        """Saves the latest commit log entry."""
        self._append_to_json_file(self.commit_log_file, self.commit_log[-1])

    def _load_or_initialize_json(self, file_path, default):
        """Loads JSON data or initializes it if the file is missing/corrupted."""
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"{file_path} not found or corrupted. Initializing.")
            with open(file_path, 'w') as f:
                json.dump(default, f)
            return default

    def _append_to_json_file(self, file_path, data):
        """Appends a single entry to a JSON file."""
        try:
            with open(file_path, 'r') as f:
                logs = json.load(f)
                if not isinstance(logs, list):
                    logs = []
        except (FileNotFoundError, json.JSONDecodeError):
            logs = []

        logs.append(data)
        with open(file_path, 'w') as f:
            json.dump(logs, f, indent=4)

    # ------------------- Account Management -------------------

    def load_account_balance(self):
        """Loads account balance or initializes it to 0 if the file is missing."""
        try:
            with open(self.account_file, 'r') as f:
                self.account_balance = float(f.read().strip())
        except (FileNotFoundError, ValueError):
            print(f"{self.account_file} not found or corrupted. Initializing to 0.")
            self.account_balance = 0
            self.save_account_balance()

    def save_account_balance(self):
        """Saves the current account balance."""
        with open(self.account_file, 'w') as f:
            f.write(str(self.account_balance))

    def get_account_balance(self):
        """Returns the current account balance."""
        return {'status': 'success', 'node_name': self.name, 'balance': self.account_balance}

    def set_account_balance(self, value):
        """Sets the account balance and replicates to followers."""
        if self.state != 'Leader':
            return {'status': 'error', 'message': 'Not the leader'}
            
        self.account_balance = value
        self.save_account_balance()
        
        # Replicate to followers
        self.replicate_to_cluster('account_balance', self.account_balance)
        
        print(f"[{self.name}] Account balance set to: {value}")
        return {'status': 'success'}

    # ------------------- Transaction Management -------------------

    def prepare_transaction(self, delta):
        """Checks if the transaction can be prepared (sufficient balance)."""
        if self.account_balance + delta < 0:
            print('Insufficient funds. Aborting transaction.')
            return False
        return True

    def commit_transaction(self, delta):
        """Commits the transaction by updating the account balance."""
        self.account_balance += delta
        self.save_account_balance()

    def prepare_log_entry(self, data):
        """Creates a log entry for a transaction."""
        entry = {
            'transaction_id': self.transaction_id,
            'simulation_num': data.get('simulation_num', 0),  # Use 'get' to prevent KeyError
            'transactions': data['transactions']
        }
        return entry
    
    def check_transaction_status(self):
        """Check the status of the last transaction."""
        if self.transaction_status:
            return {'status': self.transaction_status}
        return {'status': 'No transaction has been executed yet.'}
    
    # ------------------- 2PC Utilities -------------------
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
        print(f"[{self.name}] Node rejoining cluster with logs: {self.prepare_log}, {self.commit_log}")
        
    def get_logs_for_coordinator(self):
        """Get logs for the coordinator for recovery."""
        response = {'all_logs': {'prepare_log': self.prepare_log, 'commit_log': self.commit_log, 'raft_log': self.log}}
        return response
        

    # ------------------- 2PC Handlers -------------------

    def handle_2pc_prepare(self, data):
        """Handles the prepare phase of 2PC."""
        if self.state != 'Leader':
            print(f"[{self.name}] Not the cluster leader, rejecting 2PC prepare")
            return {'status': 'error', 'message': 'Not the cluster leader'}

        # Get transaction for this cluster
        cluster_delta = data['transactions'].get(f'Account{self.cluster_name}', 0)
        simulation_num = data.get('simulation_num', 0)
        
        print(f'[{self.name}] Processing prepare for cluster {self.cluster_name} with delta: {cluster_delta}')

        if self.prepare_transaction(cluster_delta):
            # Increment transaction ID only during prepare phase and only once.
            self.transaction_id += 1
            log_entry = self.prepare_log_entry({'transactions': data['transactions'], 'simulation_num': simulation_num})
            self.prepare_log.append(log_entry)
            # Replicate to RAFT followers
            self.replicate_to_cluster('prepare_log', log_entry)
            # Save the the prepare consensus in the persisten prepare log file
            self.save_prepare_log()
            print("Prepare phase successfully logged for all participants.")
            
            if simulation_num == SimulationScenario.CRASH_BEFORE_PREPARE.value:
                self.simulate_crash_sleep()
                print("Simulated crash scenario. Aborting transaction.")
                return {'status': 'abort'}
            
            return {'status': 'prepared'}
        return {'status': 'abort'}

    def handle_2pc_commit(self, data):
        """Handle commit phase of 2PC"""
        simulation_num = data.get('simulation_num', 0)
        if simulation_num == SimulationScenario.CRASH_BEFORE_COMMIT.value:
            self.simulate_crash_sleep()
            print("Simulated crash scenario 2. Aborting transaction.")
            return {'status': 'abort'}
        
        if self.state != 'Leader':
            print(f"[{self.name}] Not the cluster leader, rejecting 2PC commit")
            return {'status': 'error', 'message': 'Not the cluster leader'}

        try:
            # Get transaction for this cluster
            account_key = f'Account{self.cluster_name}'
            cluster_delta = data['transactions'].get(account_key, 0)
            simulation_num = data.get('simulation_num', 0)
            log_entry = self.prepare_log_entry({'transactions': data['transactions'], 'simulation_num': simulation_num})
            self.commit_log.append(log_entry)
            
            print(f'[{self.name}] Processing commit for cluster {self.cluster_name}')
            print(f'[{self.name}] Transaction data: {data}')
            print(f'[{self.name}] Current balance: {self.account_balance}')
            
            # Update balance
            self.commit_transaction(cluster_delta)
            print(f'[{self.name}] New balance: {self.account_balance}')
            
            # Replicate to RAFT followers
            self.replicate_to_cluster('account_balance', self.account_balance)
            self.replicate_to_cluster('commit_log', log_entry)
            # Save the the prepare consensus in the persisten prepare log file
            self.save_commit_log()
            print("Commit phase successfully logged for all participants.")
            return {'status': 'committed'}
        except Exception as e:
            print(f"[{self.name}] Error in commit handling: {e}")
            return {'status': 'error', 'message': str(e)}
    
    # ------------------- 2PC Request -------------------

    def handle_2pc_request(self, data):
        """Handles the 2PC request from the coordinator."""
        if self.role != 'Coordinator':
            return {'status': 'error', 'message': 'Only the coordinator can handle 2PC requests.'}

        # Convert node-specific transactions to account-level
        account_transactions = {
            'AccountA': data['transactions'].get('nodeA1', 0),
            'AccountB': data['transactions'].get('nodeB1', 0)
        }
        
        # Update the data with account-level transactions
        data['transactions'] = account_transactions

        num_participants = len(NODES) - 1  # Exclude the coordinator
        num_prepared = 0
        num_logged_prepare = 0
        num_committed = 0
        num_logged_commit = 0
        coordinator_node_name = 'node1'
        self.transaction_status = 'started'

        print(f"Starting 2PC transaction with {num_participants} participants.")
        print(f"Simulation number: {data.get('simulation_num')}")

        # Phase 1: Prepare
        for node_name in NODES:
            if node_name != coordinator_node_name:
                node_info = NODES[node_name]
                print(f"Sending prepare request to participant: {node_name}")

                start_time = time.time()
                response = None
                while time.time() - start_time < self.timeout_duration:
                    response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_prepare', data)
                    if response:
                        break
                    time.sleep(0.1)  # Avoid busy-waiting

                if not response:
                    print(f"Participant {node_name} did not respond to prepare request in time. Aborting transaction.")
                    self.transaction_status = 'aborted'
                    return {'status': 'prepare aborted'}

                if response.get('status') == 'prepared':
                    print(f"Participant {node_name} is prepared.")
                    num_prepared += 1
                else:
                    print(f"Participant {node_name} is not prepared. Aborting transaction.")
                    self.transaction_status = 'aborted'
                    return {'status': 'prepare aborted'}

        if num_prepared < num_participants:
            print("Not all participants are prepared. Aborting transaction.")
            self.transaction_status = 'aborted'
            return {'status': 'prepare aborted'}

        # Phase 2: Log Prepare
        for node_name in NODES:
            if node_name != coordinator_node_name:
                node_info = NODES[node_name]
                print(f"Sending log prepare consensus to participant: {node_name}")

                start_time = time.time()
                response = None
                while time.time() - start_time < self.timeout_duration:
                    response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_log_prepare', data)
                    if response:
                        break
                    time.sleep(0.1)  # Avoid busy-waiting

                if not response:
                    print(f"Participant {node_name} did not respond to log prepare request in time. Aborting transaction.")
                    self.transaction_status = 'aborted'
                    return {'status': 'logging prepare aborted'}

                if response.get('status') == 'logged_prepare':
                    print(f"Participant {node_name} logged the prepare request.")
                    num_logged_prepare += 1
                else:
                    print(f"Participant {node_name} did not log the prepare request. Aborting transaction.")
                    self.transaction_status = 'aborted'
                    return {'status': 'logging prepare aborted'}

        if num_logged_prepare < num_participants:
            print("Not all participants logged the prepare request. Aborting transaction.")
            self.transaction_status = 'aborted'
            return {'status': 'logging prepare aborted'}

        # Log the prepare consensus
        log_entry = self.prepare_log_entry(data)
        self.prepare_log.append(log_entry)
        self.save_prepare_log()
        print("Prepare phase successfully logged for all participants.")


        # Phase 3: Commit
        for node_name in NODES:
            if node_name != coordinator_node_name:
                node_info = NODES[node_name]
                print(f"Sending commit request to participant: {node_name}")

                start_time = time.time()
                response = None
                while time.time() - start_time < self.timeout_duration:
                    response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_commit', data)
                    if response:
                        break
                    time.sleep(0.1)  # Avoid busy-waiting

                if not response:
                    print(f"Participant {node_name} did not respond to commit request in time. Aborting transaction.")
                    self.transaction_status = 'commit aborted'
                    return {'status': 'aborted'}

                if response.get('status') == 'committed':
                    print(f"Participant {node_name} has committed.")
                    num_committed += 1
                else:
                    print(f"Participant {node_name} has not committed. Aborting transaction.")
                    self.transaction_status = 'commit aborted'
                    return {'status': 'aborted'}

        if num_committed < num_participants:
            print("Not all participants have committed. Aborting transaction.")
            self.transaction_status = 'commit aborted'
            return {'status': 'aborted'}

        # Phase 4: Log Commit
        for node_name in NODES:
            if node_name != coordinator_node_name:
                node_info = NODES[node_name]
                print(f"Sending log commit consensus to participant: {node_name}")

                start_time = time.time()
                response = None
                while time.time() - start_time < self.timeout_duration:
                    response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_log_commit', data)
                    if response:
                        break
                    time.sleep(0.1)  # Avoid busy-waiting

                if not response:
                    print(f"Participant {node_name} did not respond to log commit request in time. Aborting transaction.")
                    self.transaction_status = 'logging commit aborted'
                    return {'status': 'logging commit aborted'}

                if response.get('status') == 'logged_commit':
                    print(f"Participant {node_name} logged the commit request.")
                    num_logged_commit += 1
                else:
                    print(f"Participant {node_name} did not log the commit request. Aborting transaction.")
                    self.transaction_status = 'logging commit aborted'
                    return {'status': 'logging commit aborted'}

        if num_logged_commit < num_participants:
            print("Not all participants logged the commit request. Aborting transaction.")
            self.transaction_status = 'logging commit aborted'
            return {'status': 'logging commit aborted'}

        # Log the commit consensus
        log_entry = self.prepare_log_entry(data)
        self.commit_log.append(log_entry)
        self.save_commit_log()
        print("Commit phase successfully logged for all participants.")

        self.transaction_status = 'committed'
        print("Transaction committed successfully.")
        return {'status': 'committed'}
    
    def handle_2pc_log_prepare(self, data):
        """Handles logging the prepare phase."""
        log_entry = self.prepare_log_entry(data)
        self.commit_log.append(log_entry)
        self.save_prepare_log()
        return {'status': 'logged_prepare'}

    def handle_2pc_log_commit(self, data):
        """Handles logging the commit phase."""
        log_entry = self.prepare_log_entry(data)
        self.commit_log.append(log_entry)
        self.save_commit_log()
        return {'status': 'logged_commit'}

    # ------------------- Connection Handler -------------------

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
                    # Handle RAFT-specific RPCs
                    if rpc_type == 'RaftReplicate':
                        response = self.handle_raft_replication(request['data'])
                    # Include all previous RPC handlers
                    elif rpc_type == 'RequestVote':
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
                    elif rpc_type == '2pc_request':
                        # Delegate to 2PC-specific handler
                        response = self.handle_2pc_request(request['data'])  
                    elif rpc_type == '2pc_prepare':
                        # Delegate to 2PC-specific handler
                        response = self.handle_2pc_prepare(request['data'])
                    elif rpc_type == '2pc_commit':
                        # Delegate to 2PC-specific handler
                        response = self.handle_2pc_commit(request['data'])  
                    elif rpc_type == '2pc_log_prepare':
                        # Delegate to 2PC-specific handler
                        response = self.handle_2pc_log_prepare(request['data'])
                    elif rpc_type == '2pc_log_commit':
                        # Delegate to 2PC-specific handler
                        response = self.handle_2pc_log_commit(request['data'])
                    elif rpc_type == 'GetBalance':
                        # Handle client balance requests
                        response = self.get_account_balance()
                    elif rpc_type == 'GetLeaderStatus':
                        response = {'is_leader': self.state == 'Leader'}
                    elif rpc_type == 'CheckTransactionStatus':
                        # Handle transaction status check requests
                        response = self.check_transaction_status()
                    elif rpc_type == 'SetBalance':
                        # Handle setting the account balance
                        response = self.set_account_balance(request['data']['balance'])
                    elif rpc_type == 'GetLogs':
                        # Handle log retrieval requests
                        response = self.get_logs_for_coordinator()
                    else:
                        response = {'error': 'Unknown RPC type'}

                # Send response back to client
                client_socket.sendall(json.dumps(response).encode())
                
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close()  # Ensure socket is closed even if an error occurs

    def handle_raft_replication(self, data):
        """Handles replication requests from RAFT leader."""
        try:
            data_type = data['type']
            if data_type == 'account_balance':
                self.account_balance = data['data']
                self.save_account_balance()
                print(f"[{self.name}] Updated balance to {self.account_balance}")
            elif data_type == 'prepare_log':
                self.prepare_log.append(data['data'])
                self._append_to_json_file(self.prepare_log_file, data['data'])
            elif data_type == 'commit_log':
                self.commit_log.append(data['data'])
                self._append_to_json_file(self.commit_log_file, data['data'])
            return {'status': 'success'}
        except Exception as e:
            print(f"[{self.name}] Error in replication: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def replicate_to_cluster(self, data_type, data):

        if not self.cluster_nodes or self.state != 'Leader':
            print(f"[{self.name}] Not replicating: not leader or no cluster nodes")
            return
        
        print(f"[{self.name}] Replicating {data_type} to cluster members")
        
        for node_name, node_info in self.cluster_nodes.items():
            if node_name != self.name:
                try:
                    #--------------- RPC with timeout ---------------
                    start_time = time.time()
                    response = None
                    while time.time() - start_time < self.timeout_duration:
                        response = self.send_rpc(node_info['ip'], node_info['port'], 'RaftReplicate', {'type': data_type, 'data': data})
                        if response:
                            break
                        time.sleep(0.1)  # Avoid busy-waiting
                    #------------------------------------------------                    
                    if not response:
                        print(f"No response from leader {node_name} during {data_type} phase")
                        break
                
                    if response and response.get('status') == 'success':
                        print(f"[{self.name}] Successfully replicated to {node_name}")
                except Exception as e:
                    print(f"[{self.name}] Error replicating to {node_name}: {e}")

    def apply_entry_to_state_machine(self, entry):
        super().apply_entry_to_state_machine(entry)
        if self.role == 'Participant':
            self.commit_transaction(entry.get('delta', 0))
