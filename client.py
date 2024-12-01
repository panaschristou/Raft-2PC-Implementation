import socket
import json
import sys
from config import NODES

def send_transaction(transaction):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            coordinator = NODES['coordinator']
            s.connect((coordinator['ip'], coordinator['port']))
            s.sendall(json.dumps(transaction).encode())
            response = s.recv(4096).decode()
            return json.loads(response)
    except Exception as e:
        print(f"Error: {e}")
        return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py [transfer|bonus]")
        return
    
    command = sys.argv[1]
    
    if command == 'transfer':
        transaction = {
            'type': 'transfer',
            'amount': 100
        }
    elif command == 'bonus':
        transaction = {
            'type': 'bonus',
            'bonus_amount': None  # Will be calculated by participant
        }
    else:
        print("Invalid command")
        return
    
    response = send_transaction(transaction)
    if response and response.get('success'):
        print("Transaction completed successfully")
    else:
        print("Transaction failed")

if __name__ == '__main__':
    main()