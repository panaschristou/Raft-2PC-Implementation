import socket
import json
import threading
from account_manager import AccountManager
from config import NODES

class Participant:
    def __init__(self, name, account_name):
        self.name = name
        self.ip = NODES[name]['ip']
        self.port = NODES[name]['port']
        self.account = AccountManager(account_name)
        self.server_socket = None
        self.running = True
    
    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(5)
        
        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()
                threading.Thread(target=self.handle_request, 
                              args=(client_socket,)).start()
            except Exception as e:
                print(f"Error accepting connection: {e}")
    
    def handle_request(self, client_socket):
        try:
            data = client_socket.recv(4096).decode()
            request = json.loads(data)
            
            if request['type'] == 'prepare':
                success = self.account.prepare_transaction(request['transaction'])
                response = {'ready': success}
            elif request['type'] == 'commit':
                success = self.account.commit_transaction()
                response = {'committed': success}
            elif request['type'] == 'abort':
                self.account.abort_transaction()
                response = {'aborted': True}
            
            client_socket.sendall(json.dumps(response).encode())
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            client_socket.close()