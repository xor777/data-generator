import csv
import sys
import time
from array import array
from collections import defaultdict

def analyze_top_users(csv_file: str):
    try:
        start_time = time.time()
        
        amounts = defaultdict(lambda: array('d', [0.0]))
        counts = defaultdict(int)
        total_sum = 0.0
        total_rows = 0
        invalid_rows = 0
        
        with open(csv_file, 'r', buffering=16*1024*1024) as f:
            next(f)
            
            for line in f:
                total_rows += 1
                try:
                    parts = line.split(',')
                    user_id = int(parts[1])
                    amount = float(parts[2])
                    
                    amounts[user_id][0] += amount
                    counts[user_id] += 1
                    total_sum += amount
                    
                except (ValueError, IndexError):
                    invalid_rows += 1
                    continue
                
                if total_rows % 1000000 == 0:
                    print(f"\rОбработано строк: {total_rows:,}", end='')
        
        top_users = sorted(
            [(user_id, amounts[user_id][0], counts[user_id]) 
             for user_id in amounts],
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
        
        print(f"\nВсего пользователей: {len(amounts):,}")
        print(f"Всего транзакций: {total_rows:,}")
        print(f"Общая сумма всех транзакций: {total_sum:,.2f}")
        
        if invalid_rows:
            print(f"\nВнимание: найдено {invalid_rows:,} транзакций с невалидными данными!")
        
        end_time = time.time()
        print(f"\nВремя выполнения: {end_time - start_time:.2f} секунд")
            
    except Exception as e:
        print(f"\nОшибка при обработке файла: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python analyze_transactions_pure.py <path_to_csv>")
        sys.exit(1)
        
    csv_file = sys.argv[1]
    analyze_top_users(csv_file) 