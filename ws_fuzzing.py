import asyncio
import websockets
import random
from datetime import datetime, timedelta
import json
import argparse

class FuzzingDataGenerator:
    def __init__(self, error_rate=0.1):
        self.start_date = datetime(2024, 1, 1)
        self.error_rate = error_rate
        
    def generate_fuzzed_value(self, field_type):
        fuzz_types = {
            'string': [
                lambda: 'a' * 1000000,  
                lambda: '', 
                lambda: None,
                lambda: '"\'\\/\b\f\n\r\t',
                lambda: '诶比伊艾弗吉',
                lambda: '★☆⚡⚔',
                lambda: ' ',
                lambda: '\x00\x01\x02\x03'
            ],
            'number': [
                lambda: float('inf'),
                lambda: float('-inf'),
                lambda: float('nan'),
                lambda: 2**53 + 1,
                lambda: -(2**53 + 1),
                lambda: 2**64,
                lambda: 0.1 + 0.2,
                lambda: -0,
                lambda: 1e308,
                lambda: 1e-308
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
        return {
            'transaction_id': ''.join(random.choices('0123456789', k=20)),
            'user_id': random.randint(1, 9999),
            'amount': round(min(100000, 10 ** random.uniform(0, 5)), 2),
            'date': (self.start_date + timedelta(
                days=random.randint(0, 365),
                seconds=random.randint(0, 86399)
            )).strftime('%Y-%m-%d %H:%M:%S')
        }

    def generate_fuzzed_transaction(self):
        if random.random() < 0.005:
            return f"{{'invalid': 'json"
            
        transaction = self.generate_valid_transaction()
        
        if random.random() < self.error_rate:
            field = random.choice(['transaction_id', 'user_id', 'amount', 'date'])
            field_type = {
                'transaction_id': 'string',
                'user_id': 'number',
                'amount': 'number',
                'date': 'date'
            }[field]
            
            transaction[field] = self.generate_fuzzed_value(field_type)
            
        if random.random() < self.error_rate:
            transaction['extra_field'] = self.generate_fuzzed_value(
                random.choice(['string', 'number', 'date'])
            )
            
        if random.random() < self.error_rate:
            field_to_remove = random.choice(list(transaction.keys()))
            del transaction[field_to_remove]
            
        return transaction

async def data_stream(websocket, path, delay_ms):
    generator = FuzzingDataGenerator()
    
    try:
        while True:
            transaction = generator.generate_fuzzed_transaction()
            
            if isinstance(transaction, dict):
                try:
                    await websocket.send(json.dumps(transaction))
                except Exception:
                    await websocket.send(str(transaction))
            else:
                await websocket.send(transaction)
            
            await asyncio.sleep(delay_ms / 1000)
            
    except websockets.exceptions.ConnectionClosed:
        print("\nclient disconnected")
    except Exception as e:
        print(f"\nerror: {e}")

async def main(delay_ms):
    server = await websockets.serve(
        lambda ws, path: data_stream(ws, path, delay_ms),
        'localhost',
        8888,
        max_size=None
    )
    print(f"WS server started at ws://localhost:8888 with {delay_ms}ms delay")
    await server.wait_closed()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--delay', type=int, default=100, 
                      help='delay between messages in milliseconds')
    args = parser.parse_args()
    
    asyncio.run(main(args.delay))