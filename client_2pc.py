from client import BaseClient
from config import NODES
import json
import sys

class Client2PC(BaseClient):
    """
    Extended client class for Two-Phase Commit (2PC) functionality.
    """

    def perform_transaction(self, transactions, bonus = False, simulation_num = 0):
        """
        Send a transaction request to the coordinator.
        The transaction format is a dictionary where keys are node names
        and values are the transaction deltas for their accounts.
        Example: {"node2": -100, "node3": 100}
        """
        
        # SPLIT THE TRANSACTION INTO TWO PHASES
        # CREATE AND CALL A FUNCTION TO DO THE PREPARE. IF IT RETURNS TRUE, THEN CALL THE COMMIT
        # THIS SHOULD ALLOW US TO CHECK THE STATUS OF THE TRANSACTION
        # IF THE PREPARE RETURNS FALSE, THEN WE SHOULD ABORT THE TRANSACTION
        # IF THE COMMIT RETURNS FALSE, THEN WE SHOULD ABORT THE TRANSACTION
        # WE SHOULD BE ABLE TO CHECK THE STATUS OF THE TRANSACTION IN BETWEEN THE PREPARATION AND COMMIT
        # WE SHOULD BE ABLE TO CHECK THE STATUS OF THE TRANSACTION AFTER THE COMMIT
        
        if bonus == True:
            print("Performing transaction with bonus...")
            balance_a, balance_b, bones_value = self.calculate_bonus()
            print(f"Balance A: {balance_a}, Balance B: {balance_b}, Bonus Value: {bones_value}")
        
        coordinator = 'node1'  # Assuming node1 is the coordinator
        coordinator_info = NODES[coordinator]

        if bonus == True and bones_value != None:
            transactions = {'node2': bones_value, 'node3': bones_value}
            print(f"Sending bonus transaaction to coordinator: {transactions}")
            response = self.send_rpc(coordinator_info['ip'], coordinator_info['port'], '2pc_request', {'transactions': transactions, 'simulation_num': simulation_num})
        else:
            print(f"Sending transaction to coordinator: {transactions}")
            response = self.send_rpc(coordinator_info['ip'], coordinator_info['port'], '2pc_request', {'transactions': transactions, 'simulation_num': simulation_num})

        if response and response.get('status') == 'committed':
            print("Transaction successfully committed.")
        elif response and response.get('status') == 'aborted':
            print("Transaction aborted.")
        else:
            print("Failed to process the transaction.")
            
    def calculate_bonus(self):
        bonus_value = None
        balance_a = None
        balance_b = None
        
        for node_name in NODES:
            node_info = NODES[node_name]
            response = self.send_rpc(node_info['ip'], node_info['port'], 'GetBalance', {})
            if response and response.get('status') == 'success':
                print(f"Successfully calculated bonus for {node_name}")
            else:
                print(f"Failed to calculate bonus for {node_name}")
            if response and node_name == 'node2':
                balance_a = response.get('balance')
                if balance_a > 0:
                    bonus_value = balance_a * 0.2
                else:
                    print('Balance is not greater than 0')
            elif response and node_name == 'node3':
                balance_b = response.get('balance')
        
        return balance_a, balance_b, bonus_value  

    def check_transaction_status(self):
        """
        Request the transaction status from the coordinator.
        Useful for debugging or ensuring that the transaction was successful.
        """
        coordinator = 'node1'  # Assuming node1 is the coordinator
        coordinator_info = NODES[coordinator]

        print("Checking transaction status...")
        response = self.send_rpc(coordinator_info['ip'], coordinator_info['port'], 'CheckTransactionStatus', {})
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
            response = self.send_rpc(node_info['ip'], node_info['port'], 'GetBalance', {})
            if response:
                balance = response.get('balance', 'Could not be retrieved.')
                print(f"Account balance at {node_name}: {balance}")
            else:
                print(f"Failed to fetch balance from {node_name}")
    
    def set_account_balance(self, node, balance):
        """
        Set the account balance for a participant node.
        """
        node_info = NODES[node]
        response = self.send_rpc(node_info['ip'], node_info['port'], 'SetBalance', {'balance': balance})
        if response and response.get('status') == 'success':
            print(f"Successfully set balance for {node} to {balance}")
        else:
            print(f"Failed to set balance for {node}")

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
            transactions = {'node2': node2_val, 'node3': node3_val}
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