import requests
import os
import psycopg2
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv


load_dotenv()

def fetch_data():
    url = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def load_data(data):
    connection = psycopg2.connect(
        host=os.getenv('host'),
        port=os.getenv('port'),
        dbname=os.getenv('db'),
        user=os.getenv('user'),
        password=os.getenv('pass')
    )
    cursor = connection.cursor()

    for item in data:
        cannabis_id = item['id']
        strain = item['strain']
        type = item['type']
        rating = item['rating']
        description = item['description']
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        insert_query = """
            INSERT INTO cannabis (id, strain, type, rating, description, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        record_to_insert = (cannabis_id, strain, type, rating, description, created_at)
        cursor.execute(insert_query, record_to_insert)

    connection.commit()
    cursor.close()
    connection.close()

def main():
    while True:
        try:
            data = fetch_data()

            if data:
                # Загрузка данных в PostgreSQL
                load_data(data)
                print("Данные успешно загружены в PostgreSQL.")

            # Ожидание 12 часов для следующей загрузки данных
            time.sleep(12 * 60 * 60)

        except Exception as e:
            print(f"Произошла ошибка: {str(e)}")

if __name__ == '__main__':
    main()