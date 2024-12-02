import socket
import json
from node import Node
from config import NODES
import time
from config import SimulationScenario
class TwoPhaseCommitNode(Node):
    def __init__(self, name, role):
        super().__init__(name)
        self.role = role  # Coordinator or Participant
        self.account_balance = 0
        self.transaction_id = 0
        self.transaction_status = None
        self.timeout_duration = 1
        self.prepare_log = []
        self.commit_log = []
        self.commit_log_file = f"{self.name}_commit_log.json"
        self.prepare_log_file = f"{self.name}_prepare_log.json"
        self.account_file = f"{self.name}_account.txt"

        if role == "Participant":
            self.load_account_balance()
        self.load_prepare_log()

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
        return {'node_name': self.name, 'balance': self.account_balance}

    def set_account_balance(self, value):
        """Sets the account balance and saves it."""
        self.account_balance = value
        self.save_account_balance()
        print(f"Account balance set to: {value}")
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
        log_entry = {
            'transaction_id': self.transaction_id,
            'simulation_num': data['simulation_num'],
            'transactions': data['transactions']
        }
        self.transaction_id += 1
        return log_entry

    # ------------------- 2PC Handlers -------------------

    def handle_2pc_prepare(self, data):
        """Handles the prepare phase of 2PC."""
        delta = data['transactions'][self.name]
        simulation_num = int(data.get('simulation_num', 0))
        print(f'simulation_num during prepare: {simulation_num}')

        if simulation_num == SimulationScenario.CRASH_BEFORE_PREPARE.value:
            print('Simulation 1: Participant crashes before preparing transaction.')
            return {'status': 'abort'}

        if self.prepare_transaction(delta):
            log_entry = self.prepare_log_entry(data)
            self.prepare_log.append(log_entry)
            return {'status': 'prepared'}
        return {'status': 'abort'}

    def handle_2pc_commit(self, data):
        """Handles the commit phase of 2PC."""
        delta = data['transactions'][self.name]
        simulation_num = int(data.get('simulation_num', 0))
        print(f'simulation_num during commit: {simulation_num}')

        log_entry = self.prepare_log_entry(data)
        self.commit_log.append(log_entry)

        if simulation_num == SimulationScenario.CRASH_BEFORE_COMMIT.value:
            print('Simulation 2: Participant crashes before committing transaction.')
            return {'status': 'abort'}

        self.commit_transaction(delta)
        return {'status': 'committed'}

    def handle_2pc_log_prepare(self, data):
        """Handles logging the prepare phase."""
        self.save_prepare_log()
        return {'status': 'logged_prepare'}

    def handle_2pc_log_commit(self, data):
        """Handles logging the commit phase."""
        self.save_commit_log()
        return {'status': 'logged_commit'}
    
    # ------------------- 2PC Request -------------------

    def handle_2pc_request(self, data):
        if self.role != 'Coordinator':
            return {'status': 'error', 'message': 'Only the coordinator can handle 2PC requests.'}

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

        # Simulation-specific handling
        if int(data['simulation_num']) in [SimulationScenario.COORDINATOR_CRASH_BEFORE_COMMIT.value, SimulationScenario.COORDINATOR_DIFFERENT_PREPARE_COMMIT_LOG.value]:
            print("Simulated crash scenario. Aborting transaction and informing client.")
            return {'status': 'aborted'}

        if int(data['simulation_num']) == SimulationScenario.COORDINATOR_RECOVERS_AFTER_PREPARE.value:
            print("Coordinator recovers and sends commit requests.")

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
                    elif rpc_type == 'CheckTransactionStatus':
                        # Handle transaction status check requests
                        response = self.check_transaction_status()
                    elif rpc_type == 'SetBalance':
                        # Handle setting the account balance
                        response = self.set_account_balance(request['data']['balance'])
                    else:
                        response = {'error': 'Unknown RPC type'}

                # Send response back to client
                client_socket.sendall(json.dumps(response).encode())
                
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close()  # Ensure socket is closed even if an error occurs
            
                    


