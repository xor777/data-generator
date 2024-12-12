import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
import argparse

class TransactionGenerator:
    def __init__(self, error_rate=0.1, duplicate_rate=0.1):
        self.start_date = datetime(2024, 1, 1)
        self.error_rate = error_rate
        self.duplicate_rate = duplicate_rate
        self.last_transaction = None
        
    def generate_fuzzed_value(self, field_type):
        fuzz_types = {
            'string': [
                lambda: 'a' * 1000000,  
                lambda: '', 
                lambda: 'NULL',
                lambda: '"\'\\/\b\f\n\r\t',
                lambda: '诶比伊艾弗吉',
                lambda: '★☆⚡⚔',
                lambda: ' ',
                lambda: '\x00\x01\x02\x03'
            ],
            'number': [
                lambda: 'inf',
                lambda: '-inf',
                lambda: 'nan',
                lambda: str(2**53 + 1),
                lambda: str(-(2**53 + 1)),
                lambda: str(2**64),
                lambda: '0.30000000000000004',
                lambda: '-0',
                lambda: '1e308',
                lambda: '1e-308'
            ],
            'date': [
                lambda: '2024-13-32 25:61:61',   
                lambda: '0000-00-00 00:00:00', 
                lambda: '9999-99-99 99:99:99', 
                lambda: '',  
                lambda: 'Not a date',
                lambda: '2024-01-01T00:00:00Z',
                lambda: '1970-01-01 00:00:00', 
                lambda: datetime.max.strftime('%Y-%m-%d %H:%M:%S')
            ]
        }
        return random.choice(fuzz_types[field_type])()

    def generate_valid_transaction(self):
        transaction_id = ''.join(random.choices('0123456789', k=20))
        user_id = random.randint(1, 9999)
        log_amount = random.uniform(0, 5) 
        amount = round(min(100000, 10 ** log_amount), 2)
        
        random_days = random.randint(0, 365)
        random_seconds = random.randint(0, 86399)
        date = self.start_date + timedelta(days=random_days, seconds=random_seconds)
        
        transaction = {
            'transaction_id': transaction_id,
            'user_id': user_id,
            'amount': amount,
            'date': date.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        self.last_transaction = transaction.copy()
        return transaction

    def generate_transaction(self, enable_fuzzing=False):
        if self.last_transaction and random.random() < self.duplicate_rate:
            return self.last_transaction
            
        transaction = self.generate_valid_transaction()
        
        if not enable_fuzzing:
            return transaction
            
        if random.random() < self.error_rate:
            field = random.choice(['transaction_id', 'user_id', 'amount', 'date'])
            field_type = {
                'transaction_id': 'string',
                'user_id': 'number',
                'amount': 'number',
                'date': 'date'
            }[field]
            
            transaction[field] = self.generate_fuzzed_value(field_type)
            
        return transaction

def get_exact_row_size():
    transaction_id = ''.join(['9'] * 20)
    user_id = '1000000' 
    amount = '999999.99'
    date = '2024-12-31 23:59:59' 
    
    row = f"{transaction_id},{user_id},{amount},{date}\n"
    return len(row.encode()) 

def transaction_to_csv_line(transaction):
    try:
        if isinstance(transaction['amount'], (int, float)):
            amount = f"{transaction['amount']:.2f}"
        else:
            amount = str(transaction['amount'])
            
        return f"{transaction['transaction_id']},{transaction['user_id']},{amount},{transaction['date']}\n"
    except Exception as e:
        return f"{str(transaction['transaction_id'])},{str(transaction['user_id'])},{str(transaction['amount'])},{str(transaction['date'])}\n"

def generate_transactions(target_size_mb, output_file='transactions.csv', error_rate=0, duplicate_rate=0):
    target_size = target_size_mb * 1024 * 1024
    
    row_size = get_exact_row_size()
    header = 'transaction_id,user_id,transaction_amount,transaction_date\n'
    header_size = len(header.encode())
    
    block_size = 1000
    
    generator = TransactionGenerator(error_rate=error_rate, duplicate_rate=duplicate_rate)
    
    with open(output_file, 'w', buffering=8192*1024) as f:
        f.write(header)
        written_size = header_size
        
        while written_size < target_size:
            block = ''
            for _ in range(block_size):
                transaction = generator.generate_transaction(enable_fuzzing=error_rate > 0)
                line = transaction_to_csv_line(transaction)
                
                if written_size + len((block + line).encode()) > target_size:
                    break
                    
                block += line
            
            if not block:  
                break
                
            f.write(block)
            written_size += len(block.encode())
            
            progress = (written_size / target_size) * 100
            print(f"\rProgress - {progress:.1f}%", end='')
    
    final_size = Path(output_file).stat().st_size
    print(f"\nGenerated file size: {final_size / (1024*1024):.2f} MB")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate CSV file with transactions')
    parser.add_argument('--size', type=float, required=True,
                        help='Target file size in MB')
    parser.add_argument('--error-rate', type=float, default=0.0,
                        help='Probability of generating fuzzy data (0.0 to 1.0)')
    parser.add_argument('--duplicate-rate', type=float, default=0.0,
                        help='Probability of duplicating previous transaction (0.0 to 1.0)')
    parser.add_argument('--output', type=str, default='transactions.csv',
                        help='Output file name')
    
    args = parser.parse_args()
    
    if not (0 <= args.error_rate <= 1) or not (0 <= args.duplicate_rate <= 1):
        print("Error: rates must be between 0.0 and 1.0")
        sys.exit(1)
        
    generate_transactions(
        args.size,
        output_file=args.output,
        error_rate=args.error_rate,
        duplicate_rate=args.duplicate_rate
    )