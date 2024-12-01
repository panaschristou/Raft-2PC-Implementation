import socket
import json
from node import Node

class TwoPhaseCommitNode(Node):
    def __init__(self, name, role):
        super().__init__(name)
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

    def prepare_transaction(self, delta):
        if self.account_balance + delta < 0:
            return False
        return True

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
        self.commit_transaction(delta)
        return {'status': 'committed'}

    def handle_2pc_request(self, data):
        if data['phase'] == 'prepare':
            return self.handle_2pc_prepare(data)
        elif data['phase'] == 'commit':
            return self.handle_2pc_commit(data)
        return {'status': 'error', 'message': 'Unknown phase'}

    # Overriding handle_client_connection to add 2PC-specific logic
    def handle_client_connection(self, client_socket: socket.socket):
        """
        Extends the Node's client connection handler to include 2PC-specific RPC types.
        """
        try:
            data = client_socket.recv(4096).decode()
            if data:
                request = json.loads(data)
                rpc_type = request['rpc_type']
                response = {}

                if rpc_type in ['2pc_prepare', '2pc_commit']:
                    # Delegate to 2PC-specific handler
                    response = self.handle_2pc_request(request['data'])
                else:
                    # Call the parent class's handler for other RPCs
                    response = super().default_rpc_handler(request)

                client_socket.sendall(json.dumps(response).encode())
        except Exception as e:
            print(f"[{self.name}] Error handling client connection: {e}")
        finally:
            client_socket.close()



