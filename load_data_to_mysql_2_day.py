import mysql.connector
import pandas as pd

# Database connection details
db_config = {
    'host': 'localhost',
    'user': 'root',  # Replace with your MySQL username
    'password': 'mysql123',  # Replace with your MySQL password
    'database': 'currency_exchange',  # Replace with your database name
}

# CSV file pathD:\InterViews\360T
csv_file_path = 'D:/InterViews/360T/batch_processing_rates.csv'  # Replace with the actual path to your CSV file
#csv_file_path = 'D:/InterViews/360T/rates_14_dec_2024_5pm_newyork.csv'
# Create a connection to the MySQL database
try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Read CSV into a pandas DataFrame
    df = pd.read_csv(csv_file_path)

    # Prepare the insert query
    insert_query = """
    INSERT INTO forex_batch_processing_2_day (event_id, event_time, ccy_couple, rate)
    VALUES (%s, %s, %s, %s)
    """

    # Convert the DataFrame into a list of tuples
    data_to_insert = df.values.tolist()

    # Insert data into the table
    cursor.executemany(insert_query, data_to_insert)
    connection.commit()

    print(f"Successfully inserted {cursor.rowcount} rows into the table.")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")
