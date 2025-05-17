from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import hashlib
import random
from datetime import datetime, timedelta

PATH = '/opt/synthetic_data'
ROWS = 100

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(weeks=1),
    'retries': 1
}

dag = DAG(
    dag_id="data_synth",
    default_args=default_args,
    schedule_interval='0 10 * * *'
)

def generate_card(execution_date, rows=ROWS):
    cards_data = []
    for _ in range(rows):
        card_num = ''.join(random.choices('0123456789', k=16))
        card_num_md5 = hashlib.md5(card_num.encode()).hexdigest()
        card_order_dt = datetime.strptime(execution_date, '%Y-%M-%d')  - timedelta(days=random.randint(1, 30))
        url = f"http://example.com/page{random.randint(1, 10)}"
        cookie = f"session_{random.randint(100000, 999999)}"
        load_date = execution_date

        cards_data.append({
            'card_num': card_num,
            'card_num_md5': card_num_md5,
            'card_order_dt': card_order_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'url': url,
            'cookie': cookie,
            'load_date': load_date
        })

    cards_df = pd.DataFrame(cards_data)
    cards_df.to_csv(f'{PATH}/cards_{execution_date}.csv', index=False, sep=';')
    
def generate_status_card(execution_date):
    cards_df = pd.read_csv(f'{PATH}/cards_{execution_date}.csv', sep=';')
    status_data = []
    
    for _ in range(ROWS):
        status = random.choice(['выдана', 'не выдана'])
        if status == 'выдана':
            row = cards_df.sample().iloc[0]  # Выбираем случайную карточку из DataFrame
            card_num = row['card_num']
            card_num_md5 = row['card_num_md5']
            datetime_status = datetime.strptime(execution_date, '%Y-%M-%d') - timedelta(days=random.randint(1, 10))

            if datetime_status <= datetime.strptime(row['load_date'], '%Y-%m-%d'):  # Проверка условия по дате
                status_data.append({
                    'status': status,
                    'datetime': datetime_status.strftime('%Y-%m-%d %H:%M:%S'),
                    'card_num': card_num,
                    'card_num_md5': card_num_md5,
                    'load_date': row['load_date']
                })
        else:
            # Для статуса "не выдана"
            card_num = ''.join(random.choices('0123456789', k=16))  # Генерируем новый номер карты
            card_num_md5 = hashlib.md5(card_num.encode()).hexdigest()
            datetime_status = datetime.strptime(execution_date, '%Y-%M-%d') - timedelta(days=random.randint(1, 10))
            load_date = execution_date

            status_data.append({
                'status': status,
                'datetime': datetime_status.strftime('%Y-%m-%d %H:%M:%S'),
                'card_num': card_num,
                'card_num_md5': card_num_md5,
                'load_date': load_date
            })

    cards_df.drop(columns='card_num').to_csv(f'{PATH}/cards_{execution_date}.csv', index=False, sep=';') # Сохраняем датафрейм с картами без номеров карт

    status_df = pd.DataFrame(status_data)
    status_df.to_csv(f'{PATH}/cards_status_{execution_date}.csv', index=False, sep=';')

def generate_transactions(execution_date):
    status_df = pd.read_csv(f'{PATH}/cards_status_{execution_date}.csv', sep=';')
    transaction_data = []

    for index, row in status_df.iterrows():
        if row['status'] == 'выдана':
            amount = round(random.uniform(1.0, 500.0), 2)  # Случайная сумма транзакции
            num_transactions = random.randint(1, 5)  # Содержит от 1 до 5 транзакций
            
            for _ in range(num_transactions):
                transaction_datetime = datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=random.randint(1, 30))
                load_date = row['load_date']

                if transaction_datetime <= datetime.strptime(load_date, '%Y-%m-%d'):  # Условие для транзакции
                    transaction_data.append({
                        'card_num': row['card_num'],
                        'amount': amount,
                        'transaction_datetime': transaction_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        'load_date': load_date
                    })

    transaction_df = pd.DataFrame(transaction_data)
    transaction_df.to_csv(f'{PATH}/transactions_{execution_date}.csv', index=False, sep=';')


synth_cards = PythonOperator(
    task_id="synth_cards",
    python_callable=generate_card,
    op_kwargs={'execution_date': '{{ ds }}'},  # Передаем execution_date
    dag=dag
)

synth_status = PythonOperator(
    task_id="synth_status",
    python_callable=generate_status_card,
    op_kwargs={'execution_date': '{{ ds }}'},  # Передаем execution_date
    dag=dag
)

synth_transactions = PythonOperator(
    task_id="synth_transactions",
    python_callable=generate_transactions,
    op_kwargs={'execution_date': '{{ ds }}'},  # Передаем execution_date
    dag=dag
)

synth_cards >> synth_status >> synth_transactions
