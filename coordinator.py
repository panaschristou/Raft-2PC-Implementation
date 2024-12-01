import socket
import json
import threading
from config import NODES, PREPARE_TIMEOUT, COMMIT_TIMEOUT

class Coordinator:
    def __init__(self):
        self.ip = NODES['coordinator']['ip']
        self.port = NODES['coordinator']['port']
        self.server_socket = None
        self.running = True
    
    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        
        while self.running:
            client_socket, _ = self.server_socket.accept()
            threading.Thread(target=self.handle_transaction, 
                          args=(client_socket,)).start()
    
    def send_to_participant(self, participant, message):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(PREPARE_TIMEOUT)
                s.connect((NODES[participant]['ip'], NODES[participant]['port']))
                s.sendall(json.dumps(message).encode())
                response = s.recv(4096).decode()
                return json.loads(response)
        except Exception as e:
            print(f"Error communicating with {participant}: {e}")
            return None
    
    def handle_transaction(self, client_socket):
        try:
            data = client_socket.recv(4096).decode()
            transaction = json.loads(data)
            
            # Phase 1: Prepare
            prepare_message = {
                'type': 'prepare',
                'transaction': transaction
            }
            
            responses = {}
            for participant in ['account_a', 'account_b']:
                response = self.send_to_participant(participant, prepare_message)
                responses[participant] = response
            
            # Check all participants are ready
            all_ready = all(r and r.get('ready', False) for r in responses.values())
            
            if all_ready:
                # Phase 2: Commit
                commit_message = {'type': 'commit'}
                commit_success = True
                
                for participant in ['account_a', 'account_b']:
                    response = self.send_to_participant(participant, commit_message)
                    if not response or not response.get('committed'):
                        commit_success = False
                        break
                
                if commit_success:
                    client_socket.sendall(json.dumps({'success': True}).encode())
                else:
                    # Abort if commit failed
                    abort_message = {'type': 'abort'}
                    for participant in ['account_a', 'account_b']:
                        self.send_to_participant(participant, abort_message)
                    client_socket.sendall(json.dumps({'success': False}).encode())
            else:
                # Abort if prepare failed
                abort_message = {'type': 'abort'}
                for participant in ['account_a', 'account_b']:
                    self.send_to_participant(participant, abort_message)
                client_socket.sendall(json.dumps({'success': False}).encode())
        finally:
            client_socket.close()