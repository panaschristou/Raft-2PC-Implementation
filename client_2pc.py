from client import BaseClient
from config import NODES
import json
import sys

class Client2PC(BaseClient):
    """
    Extended client class for Two-Phase Commit (2PC) functionality.
    """

    def perform_transaction(self, transactions):
        """
        Send a transaction request to the coordinator.
        The transaction format is a dictionary where keys are node names
        and values are the transaction deltas for their accounts.
        Example: {"node2": -100, "node3": 100}
        """
        coordinator = 'node1'  # Assuming node1 is the coordinator
        coordinator_info = NODES[coordinator]

        print(f"Sending transaction to coordinator: {transactions}")
        response = self.send_rpc(coordinator_info['ip'], coordinator_info['port'], '2pc_request', transactions)

        if response and response.get('status') == 'committed':
            print("Transaction successfully committed.")
        elif response and response.get('status') == 'aborted':
            print("Transaction aborted.")
        else:
            print("Failed to process the transaction.")

    def check_transaction_status(self):
        """
        Request the transaction status from the coordinator.
        Useful for debugging or ensuring that the transaction was successful.
        """
        coordinator = 'node1'  # Assuming node1 is the coordinator
        coordinator_info = NODES[coordinator]

        print("Checking transaction status...")
        response = self.send_rpc(coordinator_info['ip'], coordinator_info['port'], 'transaction_status', {})
        if response:
            print(f"Transaction Status: {response.get('status', 'Unknown')}")
        else:
            print("Failed to retrieve transaction status from the coordinator.")

    def get_account_balances(self):
        """
        Retrieve the current account balances from all participant nodes.
        """
        print("Fetching account balances from all participants...")
        for node_name in NODES:
            if node_name == 'node1':  # Skip the coordinator
                continue
            node_info = NODES[node_name]
            response = self.send_rpc(node_info['ip'], node_info['port'], 'get_balance', {})
            if response:
                balance = response.get('balance', 'Unknown')
                print(f"Account balance at {node_name}: {balance}")
            else:
                print(f"Failed to fetch balance from {node_name}")

if __name__ == '__main__':
    client = Client2PC()
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client_2pc.py submit [value]")
        print("  python client_2pc.py leader_change")
        print("  python client_2pc.py simulate_crash [node_name]")
        print("  python client_2pc.py print_logs")
        print("  python client_2pc.py transaction '{\"node2\": -100, \"node3\": 100}'")
        print("  python client_2pc.py check_status")
        print("  python client_2pc.py get_balances")
        sys.exit(1)

    command = sys.argv[1]

    # Commands from BaseClient
    if command == 'submit':
        if len(sys.argv) != 3:
            print("Usage: python client_2pc.py submit [value]")
            sys.exit(1)
        value = sys.argv[2]
        client.submit_value(value)
    elif command == 'leader_change':
        client.trigger_leader_change()
    elif command == 'simulate_crash':
        if len(sys.argv) != 3:
            print("Usage: python client_2pc.py simulate_crash [node_name]")
            sys.exit(1)
        node_name = sys.argv[2]
        client.simulate_crash(node_name)
    elif command == 'print_logs':
        client.print_all_logs()

    # New commands for 2PC functionality
    elif command == 'transaction':
        if len(sys.argv) != 3:
            print("Usage: python client_2pc.py transaction '{\"node2\": -100, \"node3\": 100}'")
            sys.exit(1)
        try:
            transactions = json.loads(sys.argv[2])
            client.perform_transaction(transactions)
        except json.JSONDecodeError:
            print("Invalid transaction format. Use JSON format.")
            sys.exit(1)
    elif command == 'check_status':
        client.check_transaction_status()
    elif command == 'get_balances':
        client.get_account_balances()
    else:
        print("Unknown command.")