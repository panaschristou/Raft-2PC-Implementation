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
        # if bonus:
        #     print("Performing transaction with bonus...")
        #     balance_a, balance_b, bonus_value = self.calculate_bonus()
        #     print(f"Balance A: {balance_a}, Balance B: {balance_b}, Bonus Value: {bonus_value}")
            
        #     if bonus_value is not None:
        #         transactions = {'AccountA': bonus_value, 'AccountB': bonus_value}
        
        # transactions = {
        # 'AccountA': transactions.get('AccountA', 0),
        # 'AccountB': transactions.get('AccountB', 0)
        # }
        # coordinator_info = COORDINATOR_NODE['node1']
        # print(f"Sending transaction to coordinator: {transactions}")
        # response = self.send_rpc(
        #     coordinator_info['ip'],
        #     coordinator_info['port'],
        #     '2pc_request',
        #     {'transactions': transactions, 'simulation_num': simulation_num}
        # )

        # if response and response.get('status') == 'committed':
        #     print("Transaction successfully committed.")
        # elif response and response.get('status') == 'aborted':
        #     print("Transaction aborted.")
        # else:
        #     print("Failed to process the transaction.")
        if bonus:
            print("Performing bonus transaction...")
            balance_a, balance_b, bonus_value = self.calculate_bonus()
            
            if bonus_value is not None:
                # Add bonus to both accounts
                transactions = {
                    'AccountA': bonus_value,
                    'AccountB': bonus_value
                }
                print(f"Applying bonus of {bonus_value} to both accounts")
        
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
            return True
        elif response and response.get('status') == 'aborted':
            print("Transaction aborted.")
            return False
        else:
            print("Failed to process the transaction.")
            return False

    def get_account_balances(self):
        """Retrieve current account balances from RAFT cluster leaders."""
        print("Fetching account balances...")
        
        # Get balance from Cluster A leader
        balance_a = self._get_cluster_balance('A')
        print(f"Account A balance: {balance_a}")
        
        # Get balance from Cluster B leader
        balance_b = self._get_cluster_balance('B')
        print(f"Account B balance: {balance_b}")

    def _get_cluster_balance(self, cluster_letter):
        """Helper to get balance from a cluster's leader."""
        cluster_nodes = CLUSTER_A_NODES if cluster_letter == 'A' else CLUSTER_B_NODES
        
        # First identify the leader
        for node_name, node_info in cluster_nodes.items():
            leader_response = self.send_rpc(
                node_info['ip'], 
                node_info['port'],
                'GetLeaderStatus',
                {}
            )
            
            if leader_response and leader_response.get('is_leader'):
                # Found the leader, get balance
                balance_response = self.send_rpc(
                    node_info['ip'],
                    node_info['port'],
                    'GetBalance',
                    {}
                )
                if balance_response and balance_response.get('status') == 'success':
                    return balance_response.get('balance')
                break
        
        return 0  # Return 0 if no leader found or couldn't get balance

    def set_account_balance(self, account, balance):
        cluster_letter = account[-1] if account.startswith('Account') else account
        transactions = {f'Account{cluster_letter}': int(balance)}  # Ensure balance is int
        data = {
            'transactions': transactions,
            'simulation_num': 0
        }
        coordinator_info = COORDINATOR_NODE['node1']
        response = self.send_rpc(
            coordinator_info['ip'],
            coordinator_info['port'],
            '2pc_request',
            data
        )
        if response and response.get('status') == 'committed':
            print(f"Successfully set balance for Account {cluster_letter} to {balance}")
        else:
            print(f"Failed to set balance for Account {cluster_letter}")

    def calculate_bonus(self):
        """Calculate 20% bonus based on current account balances"""
        print("Calculating bonus...")
        
        # Get current balances
        balance_a = self._get_cluster_balance('A')
        balance_b = self._get_cluster_balance('B')
        
        if balance_a is None or balance_b is None:
            print("Could not retrieve current balances")
            return None, None, None
            
        # Calculate 20% bonus
        bonus_value = int(balance_a * 0.2)  # Convert to int to avoid floating point issues
        
        print(f"Current balances - A: {balance_a}, B: {balance_b}")
        print(f"Bonus amount (20% of A): {bonus_value}")
        
        return balance_a, balance_b, bonus_value

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
