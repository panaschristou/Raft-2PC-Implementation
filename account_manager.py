import json
import os

class AccountManager:
    def __init__(self, account_name):
        self.account_name = account_name
        self.filename = f"{account_name}_balance.txt"
        self.temp_filename = f"{account_name}_temp.txt"
        self.initialize_account()
    
    def initialize_account(self):
        if not os.path.exists(self.filename):
            with open(self.filename, 'w') as f:
                f.write('0')
    
    def read_balance(self):
        with open(self.filename, 'r') as f:
            return float(f.read().strip())
    
    def write_balance(self, amount):
        with open(self.filename, 'w') as f:
            f.write(str(amount))
    
    def prepare_transaction(self, transaction):
        current_balance = self.read_balance()
        
        if transaction['type'] == 'transfer':
            if self.account_name == 'A':
                new_balance = current_balance - transaction['amount']
            else:
                new_balance = current_balance + transaction['amount']
        elif transaction['type'] == 'bonus':
            if self.account_name == 'A':
                bonus = current_balance * 0.2
                new_balance = current_balance + bonus
            else:
                bonus = transaction['bonus_amount']
                new_balance = current_balance + bonus
        
        if new_balance < 0:
            return False
            
        with open(self.temp_filename, 'w') as f:
            f.write(str(new_balance))
        return True
    
    def commit_transaction(self):
        if os.path.exists(self.temp_filename):
            with open(self.temp_filename, 'r') as f:
                new_balance = f.read()
            with open(self.filename, 'w') as f:
                f.write(new_balance)
            os.remove(self.temp_filename)
            return True
        return False
    
    def abort_transaction(self):
        if os.path.exists(self.temp_filename):
            os.remove(self.temp_filename)