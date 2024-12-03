from client import BaseClient
from config import COORDINATOR_NODE, CLUSTER_A_NODES, CLUSTER_B_NODES
import json
import sys

class Client2PC(BaseClient):
    def perform_transaction(self, transactions, bonus=False, simulation_num=0):
        """
        Send a transaction request to the coordinator.
        Now uses account-level transactions instead of node-specific ones.
        Example: {"AccountA": -100, "AccountB": 100}
        """
        if bonus:
            print("Performing transaction with bonus...")
            balance_a, balance_b, bonus_value = self.calculate_bonus()
            print(f"Balance A: {balance_a}, Balance B: {balance_b}, Bonus Value: {bonus_value}")
            
            if bonus_value is not None:
                transactions = {'AccountA': bonus_value, 'AccountB': bonus_value}

        coordinator_info = COORDINATOR_NODE['node1']
        print(f"Sending transaction to coordinator: {transactions}")
        response = self.send_rpc(
            coordinator_info['ip'],
            coordinator_info['port'],
            '2pc_request',
            {'transactions': transactions, 'simulation_num': simulation_num}
        )

        if response and response.get('status') == 'committed':
            print("Transaction successfully committed.")
        elif response and response.get('status') == 'aborted':
            print("Transaction aborted.")
        else:
            print("Failed to process the transaction.")

    def get_account_balances(self):
        """Retrieve current account balances from RAFT cluster leaders."""
        print("Fetching account balances...")
        
        # Try each node in Cluster A until we find the leader
        for node_name, node_info in CLUSTER_A_NODES.items():
            response = self.send_rpc(node_info['ip'], node_info['port'], 'GetBalance', {})
            if response and response.get('status') == 'success':
                print(f"Account A balance: {response.get('balance')}")
                break
                
        # Try each node in Cluster B until we find the leader
        for node_name, node_info in CLUSTER_B_NODES.items():
            response = self.send_rpc(node_info['ip'], node_info['port'], 'GetBalance', {})
            if response and response.get('status') == 'success':
                print(f"Account B balance: {response.get('balance')}")
                break

    def set_account_balance(self, account, balance):
        """Set account balance by finding and updating the RAFT leader."""
        # Extract cluster letter (A or B) from AccountA/AccountB format
        cluster_letter = account[-1] if account.startswith('Account') else account
        
        # Create transaction for coordinator with proper account format
        transactions = {f'Account{cluster_letter}': balance}
        
        # Send to coordinator as a transaction
        coordinator_info = COORDINATOR_NODE['node1']
        response = self.send_rpc(
            coordinator_info['ip'],
            coordinator_info['port'],
            '2pc_request',
            {'transactions': transactions}
        )
        
        if response and response.get('status') == 'committed':
            print(f"Successfully set balance for Account {cluster_letter} to {balance}")
        else:
            print(f"Failed to set balance for Account {cluster_letter}")

if __name__ == '__main__':
    client = Client2PC()
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client_2pc.py submit [value]")
        print("  python client_2pc.py leader_change")
        print("  python client_2pc.py simulate_crash [node_name]")
        print("  python client_2pc.py print_logs")
        print("  python client_2pc.py transaction trans_val1 trans_val2 [bonus flag (optional)] [node crash simulation # (Default = 0)]")
        print("  python client_2pc.py check_status")
        print("  python client_2pc.py get_balances")
        print("  python client_2pc.py set_balance [node_name] [balance]")
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
        if len(sys.argv) < 4 or len(sys.argv) > 6:
            print("Usage: python client_2pc.py transaction trans_val1 trans_val2 [bonus flag (optional)]")
            sys.exit(1)
        try:
            node2_val = int(sys.argv[2])
            node3_val = int(sys.argv[3])
            transactions = {'AccountA': node2_val, 'AccountB': node3_val}
            bonus_flag = True if len(sys.argv) > 4 and sys.argv[4] == 'bonus' else False
            crash_simulation_num = sys.argv[5] if len(sys.argv) == 6 else 0
            client.perform_transaction(transactions, bonus_flag, simulation_num=crash_simulation_num)
        except json.JSONDecodeError:
            print("Invalid transaction format. Use JSON format.")
            sys.exit(1)
    elif command == 'check_transcation_status':
        client.check_transaction_status()
    elif command == 'get_balances':
        client.get_account_balances()
    elif command == 'set_balance':
        if len(sys.argv) != 4:
            print("Usage: python client_2pc.py set_balance [node_name] [balance]")
            sys.exit(1)
        node_name = sys.argv[2]
        balance = int(sys.argv[3])
        client.set_account_balance(node_name, balance)
    else:
        print("Unknown command.")
