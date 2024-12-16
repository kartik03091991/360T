
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

# Target Date and Time: 14-Dec-2024, 5:00 PM New York Time
ny_time = datetime(2024, 12, 14, 17, 0, 0)  # New York Time (EST)
utc_time = ny_time + timedelta(hours=5)  # Convert to UTC

# Initialize data
currencies = ['EURUSD', 'NZDUSD', 'AUDUSD', 'EURGBP', 'GBPUSD']
data = []
event_id = 288230383844089595  # Starting event ID

# Generate realistic rates for each currency
rates = {
    currency: round(np.random.normal(loc=1.2, scale=0.01), 8) for currency in currencies
}

# Add data for each currency at the target time
for currency, rate in rates.items():
    data.append([event_id, int(utc_time.timestamp() * 1000), currency, rate])
    event_id += 1

# Create a DataFrame
df = pd.DataFrame(data, columns=['event_id', 'event_time', 'ccy_couple', 'rate'])

# Save dataset to CSV
output_path = 'D:/InterViews/360T/rates_14_dec_2024_5pm_newyork.csv'
df.to_csv(output_path, index=False)

print(f"Dataset saved to: {output_path}")