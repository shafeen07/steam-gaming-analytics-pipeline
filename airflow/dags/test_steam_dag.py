from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests

def test_steam_api():
    """Test Steam API connection and store result"""
    import psycopg2
    
    url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=730"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        player_count = data['response']['player_count']
        
        # Store in database for verification
        conn = psycopg2.connect(
            host="postgres",
            database="airflow", 
            user="airflow",
            password="airflow"
        )
        cur = conn.cursor()
        
        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS steam_test_results (
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                game_name VARCHAR(50),
                player_count INTEGER
            )
        """)
        
        # Insert the data
        cur.execute(
            "INSERT INTO steam_test_results (game_name, player_count) VALUES (%s, %s)",
            ("CS2", player_count)
        )
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"SUCCESS: CS2 current players: {player_count}")
        return player_count
        
    except Exception as e:
        print(f"Error calling Steam API: {e}")
        raise

default_args = {
    'owner': 'steam-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'test_steam_connection',
    default_args=default_args,
    description='Test Steam API connection',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['steam', 'test']
)

test_api = PythonOperator(
    task_id='test_steam_api',
    python_callable=test_steam_api,
    dag=dag
)

print_success = BashOperator(
    task_id='print_success',
    bash_command='echo "Steam API test completed successfully!"',
    dag=dag
)

test_api >> print_success
