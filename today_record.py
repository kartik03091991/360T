import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

# Parameters for the dataset
job_trigger_time = datetime(2024, 12, 15, 10, 30, 0, tzinfo=timezone.utc)  # Job trigger time (UTC)
start_time = job_trigger_time - timedelta(hours=1)  # Data starts 1 hour before the job
currencies = ['EURUSD', 'NZDUSD', 'AUDUSD', 'EURGBP', 'GBPUSD']  # Currency pairs
data = []
event_id = 288230383844089600  # Starting event ID

# Function to simulate realistic rate changes
def generate_realistic_rate(previous_rate):
    return round(np.random.normal(loc=previous_rate, scale=0.001), 8)  # Small variation

# Initialize starting rates for each currency pair
rates = {currency: round(np.random.uniform(1.0, 1.5), 8) for currency in currencies}

# Generate data for 3 active currencies
active_currencies = currencies[:3]  # First 3 currencies will have active rates
inactive_currencies = currencies[3:]  # Last 2 currencies will not have active rates

# Generate data from start_time to job_trigger_time
current_time = start_time
while current_time <= job_trigger_time:
    for currency in currencies:
        # Active currencies get updates every 10-30 seconds
        if currency in active_currencies:
            updates = np.random.randint(1, 5)  # Number of updates in this time frame
            for _ in range(updates):
                timestamp = current_time + timedelta(milliseconds=np.random.randint(0, 1000))  # Randomize timestamp
                rate = generate_realistic_rate(rates[currency])
                rates[currency] = rate
                data.append([event_id, int(timestamp.timestamp() * 1000), currency, rate])
                event_id += 1  # Increment event_id uniquely for each row
        # Inactive currencies get updates only before the active window
        elif current_time < job_trigger_time - timedelta(seconds=30):
            updates = np.random.randint(1, 3)  # Few updates outside the active window
            for _ in range(updates):
                timestamp = current_time + timedelta(milliseconds=np.random.randint(0, 1000))  # Randomize timestamp
                rate = generate_realistic_rate(rates[currency])
                rates[currency] = rate
                data.append([event_id, int(timestamp.timestamp() * 1000), currency, rate])
                event_id += 1  # Increment event_id uniquely for each row
    current_time += timedelta(seconds=10)  # Increment by 10 seconds

# Create a DataFrame
df = pd.DataFrame(data, columns=['event_id', 'event_time', 'ccy_couple', 'rate'])

# Save dataset to CSV
output_path = 'D:/InterViews/360T/batch_processing_rates.csv'
df.to_csv(output_path, index=False)
print(f"Dataset saved to: {output_path}")
