from kafka import KafkaProducer, KafkaConsumer
from flask import Flask, render_template
from flask_socketio import SocketIO
import json
import pandas as pd
from datetime import datetime, timedelta
import mysql.connector
import time

# Flask and SocketIO Setup
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# MySQL Configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'mysql123',
    'database': 'Currency_Exchange'
}

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'forex_rates_stream'

# In-memory store for active rates
active_rates = {}

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def fetch_yesterday_data():
    """Fetch all currency rates for yesterday's 5 PM New York time."""
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        yesterday = datetime.utcnow() - timedelta(days=1)
        start_time = int((yesterday.replace(hour=17, minute=0, second=0, microsecond=0)).timestamp() * 1000)
        end_time = start_time + 3600000  # One-hour window around 5 PM
        
        query = """
        SELECT ccy_couple, rate, event_time FROM forex_batch_processing_2_day
        WHERE event_time BETWEEN %s AND %s
        """
        cursor.execute(query, (start_time, end_time))
        return cursor.fetchall()
    finally:
        cursor.close()
        connection.close()

def fetch_today_data():
    """Continuously fetch today's data from the database."""
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)
        while True:
            today = datetime.utcnow().date()
            start_time = int(datetime.combine(today, datetime.min.time()).timestamp() * 1000)
            now_time = int(datetime.utcnow().timestamp() * 1000)

            query = """
            SELECT ccy_couple, rate, event_time FROM forex_batch_processing_2_day
            WHERE event_time BETWEEN %s AND %s
            """
            cursor.execute(query, (start_time, now_time))
            for row in cursor.fetchall():
                producer.send(KAFKA_TOPIC, row)
                time.sleep(0.1)  # Simulate real-time data streaming
    finally:
        cursor.close()
        connection.close()

def process_stream():
    """Process streaming data, calculate active rates and percentage change."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    while True:
        for message in consumer:
            data = message.value
            ccy_couple = data['ccy_couple']
            rate = data['rate']
            event_time = data['event_time']

            # Update active rates (last 30 seconds)
            active_rates[ccy_couple] = {'rate': rate, 'time': event_time}

            # Fetch yesterday's rate for percentage change calculation
            yesterday_rate = fetch_yesterday_rate(ccy_couple)
            if yesterday_rate:
                percentage_change = round(((rate - yesterday_rate) / yesterday_rate) * 100, 3)
            else:
                percentage_change = None

            # Prepare the real-time update
            update = {
                'ccy_couple': ccy_couple,
                'current_rate': rate,
                'percentage_change': f"{percentage_change}%" if percentage_change else "N/A",
                'event_time': datetime.fromtimestamp(event_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
            }

            # Emit data to the front end using SocketIO
            socketio.emit('forex_update', update)

# Flask Route for Real-Time Dashboard
@app.route('/')
def index():
    return render_template('index.html')  # HTML page to display real-time data

# Run Producer and Consumer in Background Threads
@socketio.on('connect')
def start_streaming():
    print("Client connected, starting stream...")
    socketio.start_background_task(fetch_today_data)  # Producer for today's data
    socketio.start_background_task(process_stream)    # Consumer for streaming data

if __name__ == "__main__":
    # Load yesterday's data and send to Kafka
    yesterday_data = fetch_yesterday_data()
    for row in yesterday_data:
        producer.send(KAFKA_TOPIC, row)

    # Start Flask application
    socketio.run(app, debug=True)

