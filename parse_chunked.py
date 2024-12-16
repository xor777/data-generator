import pandas as pd
import sys
import time
from typing import Dict, Tuple

def process_chunk(chunk: pd.DataFrame, totals: Dict[int, Tuple[float, int]]):
    for _, row in chunk.iterrows():
        try:
            user_id = int(row['user_id'])
            amount = float(row['transaction_amount'])
            
            if user_id in totals:
                total_sum, count = totals[user_id]
                totals[user_id] = (total_sum + amount, count + 1)
            else:
                totals[user_id] = (amount, 1)
                
        except (ValueError, TypeError):
            continue
    
    return totals

def analyze_top_users(csv_file: str, chunk_size: int = 100000):
    try:
        start_time = time.time()
        user_totals = {}
        total_rows = 0
        
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            total_rows += len(chunk)
            user_totals = process_chunk(chunk, user_totals)
            print(f"\rОбработано строк: {total_rows:,}", end='')
            
        top_users = sorted(
            [(user_id, total, count) 
             for user_id, (total, count) in user_totals.items()],
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        print("\n\nТоп 5 пользователей по сумме транзакций:")
        print("-" * 80)
        print(f"{'User ID':<10} {'Total Amount':>15} {'Transactions':>15} {'Avg Amount':>15}")
        print("-" * 80)
        
        for user_id, total, count in top_users:
            avg = total / count
            print(f"{user_id:<10} {total:>15.2f} {count:>15d} {avg:>15.2f}")
            
        total_sum = sum(total for _, total, _ in user_totals.items())
        print(f"\nВсего пользователей: {len(user_totals):,}")
        print(f"Всего транзакций: {total_rows:,}")
        print(f"Общая сумма всех транзакций: {total_sum:,.2f}")
        
        end_time = time.time()
        print(f"\nВремя выполнения: {end_time - start_time:.2f} секунд")
            
    except Exception as e:
        print(f"\nОшибка при обработке файла: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python analyze_transactions_chunked.py <path_to_csv>")
        sys.exit(1)
        
    csv_file = sys.argv[1]
    analyze_top_users(csv_file) 