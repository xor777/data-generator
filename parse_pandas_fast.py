import pandas as pd
import sys
import time

def analyze_top_users(csv_file: str):
    try:
        start_time = time.time()
        
        df = pd.read_csv(csv_file, usecols=['user_id', 'transaction_amount'])
        df['amount_clean'] = pd.to_numeric(df['transaction_amount'], errors='coerce')
        
        user_totals = df.groupby('user_id')['amount_clean'].agg([
            'sum',
            'count',
            'mean'
        ]).reset_index()
        
        top_users = user_totals.nlargest(5, 'sum')
        
        print("\nТоп 5 пользователей по сумме транзакций:")
        print("-" * 80)
        print(f"{'User ID':<10} {'Total Amount':>15} {'Transactions':>15} {'Avg Amount':>15}")
        print("-" * 80)
        
        for _, row in top_users.iterrows():
            print(f"{row['user_id']:<10} {row['sum']:>15.2f} {row['count']:>15.0f} {row['mean']:>15.2f}")
            
        print(f"\nВсего пользователей: {len(user_totals):,}")
        print(f"Всего транзакций: {len(df):,}")
        print(f"Общая сумма всех транзакций: {df['amount_clean'].sum():,.2f}")
        
        invalid_amounts = df[pd.isna(df['amount_clean'])].shape[0]
        if invalid_amounts > 0:
            print(f"\nВнимание: найдено {invalid_amounts:,} транзакций с невалидными суммами!")
            
        end_time = time.time()
        print(f"\nВремя выполнения: {end_time - start_time:.2f} секунд")
            
    except Exception as e:
        print(f"\nОшибка при обработке файла: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python analyze_transactions_fast.py <path_to_csv>")
        sys.exit(1)
        
    csv_file = sys.argv[1]
    analyze_top_users(csv_file) 