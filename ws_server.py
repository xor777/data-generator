import asyncio
import websockets
import random
from datetime import datetime, timedelta
import json
import argparse

class DataGenerator:
    def __init__(self):
        self.start_date = datetime(2024, 1, 1)
    
    def generate_transaction(self):
        transaction_id = ''.join(random.choices('0123456789', k=20))
        user_id = random.randint(1, 9999)
        
        log_amount = random.uniform(0, 5)
        amount = round(min(100000, 10 ** log_amount), 2)
        
        random_days = random.randint(0, 365)
        random_seconds = random.randint(0, 86399)
        date = self.start_date + timedelta(days=random_days, seconds=random_seconds)
        
        return {
            'transaction_id': transaction_id,
            'user_id': user_id,
            'amount': amount,
            'date': date.strftime('%Y-%m-%d %H:%M:%S')
        }

async def data_stream(websocket, path, delay_ms):
    generator = DataGenerator()
    
    try:
        while True:
            transaction = generator.generate_transaction()
            await websocket.send(json.dumps(transaction))
            
            # delay
            await asyncio.sleep(delay_ms / 1000)
            
    except websockets.exceptions.ConnectionClosed:
        print("\nClient disconnected")
    except Exception as e:
        print(f"\nError: {e}")

async def main(delay_ms):
    server = await websockets.serve(
        lambda ws, path: data_stream(ws, path, delay_ms),
        'localhost',
        8888,
        max_size=None
    )
    print(f"WebSocket server started at ws://localhost:8888 with {delay_ms}ms delay")
    await server.wait_closed()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--delay', type=int, default=100, 
                      help='Delay between messages in milliseconds')
    args = parser.parse_args()
    
    asyncio.run(main(args.delay))