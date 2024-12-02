import socket
import json
from node import Node
from config import NODES
class TwoPhaseCommitNode(Node):
    def __init__(self, name, role):
        super().__init__(name)
        self.role = role  # Coordinator or Participant
        self.account_balance = 0
        self.transaction_id = 0
        self.transaction_status = None
        self.prepare_log = []
        self.commit_log = []
        self.commit_log_file = f'{self.name}_commit_log.json'
        self.prepare_log_file = f'{self.name}_prepare_log.json'
        self.account_file = f"{self.name}_account.txt"
        if role == "Participant":
            self.load_account_balance()
        self.load_prepare_log()
        
    def load_prepare_log(self):
        try:
            with open(self.prepare_log_file, 'r') as f:
                self.prepare_log = json.load(f)
                self.transaction_id = self.prepare_log[-1]['transaction_id'] + 1
                if not isinstance(self.prepare_log, list):
                    self.prepare_log = []
        except (FileNotFoundError, json.JSONDecodeError):
            self.prepare_log = []
            
    def save_prepare_log(self):
        try:
            with open(self.prepare_log_file, 'r') as f:
                logs = json.load(f)
                if not isinstance(logs, list):
                    logs = []
        except (FileNotFoundError, json.JSONDecodeError):
            logs = []

        logs.append(self.prepare_log[-1])

        with open(self.prepare_log_file, 'w') as f:
            json.dump(logs, f)
            
    def load_commit_log(self):
        try:
            with open(self.commit_log_file, 'r') as f:
                self.commit_log = json.load(f)
                if not isinstance(self.commit_log, list):
                    self.commit_log = []
        except (FileNotFoundError, json.JSONDecodeError):
            self.commit_log = []
    
    def save_commit_log(self):
        try:
            with open(self.commit_log_file, 'r') as f:
                logs = json.load(f)
                if not isinstance(logs, list):
                    logs = []
        except (FileNotFoundError, json.JSONDecodeError):
            logs = []

        logs.append(self.commit_log[-1])

        with open(self.commit_log_file, 'w') as f:
            json.dump(logs, f)

    def load_account_balance(self):
        try:
            with open(self.account_file, 'r') as f:
                self.account_balance = float(f.read().strip())
        except FileNotFoundError:
            with open(self.account_file, 'w') as f:
                f.write("0")
            self.account_balance = 0

    def save_account_balance(self):
        with open(self.account_file, 'w') as f:
            f.write(str(self.account_balance))
    
    def get_account_balance(self):
        balance = self.account_balance
        return{'node_name': self.name, 'balance': balance}
    
    def set_account_balance(self, value):
        self.account_balance = value
        self.save_account_balance()
        print(f"Account balance set to: {value}")
        return {'status': 'success'}

    def prepare_transaction(self, delta):
        if self.account_balance + delta < 0:
            print('Insufficient funds. Aborting transaction.')
            return False
        return True
    
    def check_transaction_status(self):
        if self.role == 'Coordinator':
            transaction_status = self.transaction_status
        return {'transaction_status': transaction_status}

    def commit_transaction(self, delta):
        self.account_balance += delta
        self.save_account_balance()
    
    def prepare_log_entry(self, data):
        log_entry = {}
        log_entry['transaction_id'] = self.transaction_id
        log_entry['simulation_num'] = data['simulation_num']
        log_entry['transactions'] = data['transactions']
        self.transaction_id += 1
        return log_entry

    def handle_2pc_prepare(self, data):
        delta = data['transactions'][self.name]
        simulation_num = int(data.get('simulation_num', 0))
        print(f'simulation_num during prepare: {simulation_num}')
        
        if simulation_num == 1:
            print('Simulation 1: Participant crashes before preparing transaction')
            return {'status': 'abort'}
        
        if self.prepare_transaction(delta):
            log_entry = self.prepare_log_entry(data)
            self.prepare_log.append(log_entry)
            return {'status': 'prepared'}
        return {'status': 'abort'}

    def handle_2pc_commit(self, data):
        delta = delta = data['transactions'][self.name]
        simulation_num = int(data.get('simulation_num', 0))
        print(f'simulation_num during commit: {simulation_num}')
        
        if simulation_num == 2:
            print('Simulation 2: Participant crashes before committing transaction')
            return {'status': 'abort'}
        
        self.commit_transaction(delta)
        return {'status': 'committed'}

    def handle_2pc_log_prepare(self, data):
        self.save_prepare_log()
        return {'status': 'logged_prepare'}
    
    def handle_2pc_log_commit(self, data):
        self.save_commit_log()
        return {'status': 'logged_commit'}
    
    def handle_2pc_request(self, data):
        if self.role == 'Coordinator':
            num_participants = len(data)
            num_prepared = 0
            num_logged_prepare = 0
            num_committed = 0
            num_logged_commit = 0
            coordinator_node_name = 'node1'
            self.transaction_status = 'started'
            print(f"Starting 2PC transaction with {num_participants} participants.")
            print(f'Simulation number: {data["simulation_num"]}')
            
            for node_name in NODES:
                if node_name != coordinator_node_name:
                    node_info = NODES[node_name]
                    print(f"Sending prepare request to  participant: {node_name}")

                    response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_prepare', data)
                    if response and response.get('status') == 'prepared':
                        print(f"Participant {node_name} is prepared.")
                        num_prepared += 1
                        if num_prepared == num_participants:
                            for node_name in NODES:
                                if node_name != coordinator_node_name:
                                    node_info = NODES[node_name]
                                    print(f"Sending log prepare consencus to  participant: {node_name}")

                                    response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_log_prepare', data)
                                    if response and response.get('status') == 'logged_prepare':
                                        print(f"Participant {node_name} logged the prepare request.")
                                        num_logged_prepare += 1
                                        if num_logged_prepare == num_participants:
                                            log_entry = self.prepare_log_entry(data)
                                            self.prepare_log.append(log_entry)
                                            self.save_prepare_log()
                                    else:
                                        print(f"Participant {node_name} did not log prepare request.")
                                        return {'status': 'logging prepare aborted'}
                    else:
                        self.transaction_status = 'aborted'
                        print(f"Participant {node_name} is not prepared.")
                        return {'status': 'prepare aborted'}
            

            
            if int(data['simulation_num']) == 3:
                print('Simulation 3: Coordinator crashes before sending commit requests and did not log the transaction. Client is informed to start a new transaction.')
                return {'status': 'aborted'}
            
            if int(data['simulation_num']) == 4:
                print('Simulation 4: Coordinator crashes but can recover because he logged the transaction status and can continue after the preparation phase.')
               
                    
            if num_prepared == num_participants:
                self.transaction_status = 'prepared'
                print("All participants are prepared. Sending commit requests.")
                
                for node_name in NODES:
                    if node_name != coordinator_node_name:
                        node_info = NODES[node_name]
                        print(f"Sending commit request to participant: {node_name}")
                        
                        response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_commit', data)
                        if response and response.get('status') == 'committed':
                            print(f"Participant {node_name} has committed.")
                            num_committed += 1
                            if num_committed == num_participants:
                                for node_name in NODES:
                                    if node_name != coordinator_node_name:
                                        node_info = NODES[node_name]
                                        print(f"Sending log commit consencus to  participant: {node_name}")

                                        response = self.send_rpc(node_info['ip'], node_info['port'], '2pc_log_commit', data)
                                        if response and response.get('status') == 'logged_commit':
                                            print(f"Participant {node_name} logged the commit request.")
                                            num_logged_commit += 1
                                            if num_logged_commit == num_participants:
                                                log_entry = self.prepare_log_entry(data)
                                                self.commit_log.append(log_entry)
                                                self.save_commit_log()
                                        else:
                                            print(f"Participant {node_name} did not log commit request.")
                                            return {'status': 'logging commit aborted'}
                        else:
                            self.transaction_status = 'commit aborted'
                            print(f"Participant {node_name} has not committed.")
                            return {'status': 'aborted'}
            
            if num_committed == num_participants:
                self.transaction_status = 'committed'
                print("All participants have committed.")
                return {'status': 'committed'}
            
        return {'status': 'error', 'message': 'Transaction not committed'}

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
            
            



