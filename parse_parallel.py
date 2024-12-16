import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import mmap
import os

def process_chunk(chunk_start: int, chunk_size: int, mm: mmap.mmap, lock: Lock, amounts: dict, counts: dict) -> tuple:
    chunk_sum = 0
    chunk_invalid = 0
    rows = 0
    local_amounts = defaultdict(float)
    local_counts = defaultdict(int)
    
    pos = chunk_start
    chunk_end = chunk_start + chunk_size
    
    while pos < chunk_end:
        try:

            line_end = mm.find(b'\n', pos)
            if line_end == -1:
                break
                
            line = memoryview(mm[pos:line_end])
            
            comma1 = -1
            comma2 = -1
            comma3 = -1
            for i, char in enumerate(line):
                if char == 44:  # ,
                    if comma1 == -1:
                        comma1 = i
                    elif comma2 == -1:
                        comma2 = i
                    else:
                        comma3 = i
                        break
            
            user_id = int(line[comma1+1:comma2])
            amount = float(line[comma2+1:comma3])
            
            local_amounts[user_id] += amount
            local_counts[user_id] += 1
            chunk_sum += amount
            rows += 1
            
            pos = line_end + 1
            
        except (ValueError, IndexError):
            chunk_invalid += 1
            pos = mm.find(b'\n', pos) + 1
            if pos <= 0:
                break
    
    with lock:
        for user_id, amount in local_amounts.items():
            amounts[user_id] += amount
            counts[user_id] += local_counts[user_id]
            
    return chunk_sum, chunk_invalid, rows

def analyze_top_users(csv_file: str):
    try:
        start_time = time.time()
        amounts = defaultdict(float)
        counts = defaultdict(int)
        total_sum = 0
        total_rows = 0
        invalid_rows = 0
        lock = Lock()
        

        file_size = os.path.getsize(csv_file)
        chunk_size = max(file_size // 12, 1024*1024) 
        
        with open(csv_file, 'rb') as f:

            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            
            first_line_end = mm.find(b'\n') + 1
            
            chunk_starts = range(first_line_end, file_size, chunk_size)
            
            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = [
                    executor.submit(
                        process_chunk, 
                        chunk_start,
                        chunk_size,
                        mm,
                        lock,
                        amounts,
                        counts
                    )
                    for chunk_start in chunk_starts
                ]
                
                for future in futures:
                    chunk_sum, chunk_invalid, chunk_rows = future.result()
                    total_sum += chunk_sum
                    invalid_rows += chunk_invalid
                    total_rows += chunk_rows
                    print(f"\rОбработано строк: {total_rows:,}", end='')
            
            mm.close()
        
        top_users = sorted(
            ((user_id, amounts[user_id], counts[user_id]) 
             for user_id in amounts),
            key=lambda x: (x[1], -x[0]),
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
        print("Использование: python analyze_transactions_parallel.py <path_to_csv>")
        sys.exit(1)
        
    csv_file = sys.argv[1]
    analyze_top_users(csv_file) 