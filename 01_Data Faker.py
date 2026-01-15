import pandas as pd
import random
from datetime import datetime, timedelta
import calendar

# ------------------------------
# PARAMETERS
# ------------------------------
num_hubs = 10
num_customers = 1500
num_pickers = 60
num_riders = 60
months = list(range(1, 11))  # Jan–Oct
start_year = 2019
end_year = 2025
rows_per_chunk = 50_000  # write to CSV in chunks

# ------------------------------
# GENERATE DIMENSION DATA (no Faker)
# ------------------------------
hubs = [
    {
        'Hub ID': i,
        'City': f'City_{i}',
        'Hub Name': f'Hub_{i}',
        'Monthly Operating Cost': random.randint(10000, 50000)
    }
    for i in range(1, num_hubs + 1)
]

pickers = [{'Picker ID': i, 'Picker Name': f'Picker_{i}'} for i in range(1, num_pickers + 1)]
riders = [{'Rider ID': i, 'Rider Name': f'Rider_{i}'} for i in range(1, num_riders + 1)]
customers = [i for i in range(1, num_customers + 1)]

# ------------------------------
# CREATE CSV IN CHUNKS
# ------------------------------
csv_file = 'all_hubs_orders_5M.csv'
first_chunk = True
order_id = 1

for hub in hubs:
    for month in months:
        # ~50k rows per hub-month
        num_orders = random.randint(45_000, 55_000)
        rows = []

        for _ in range(num_orders):
            # Random valid year/month/day
            while True:
                year = random.randint(start_year, end_year)
                if year == 2025 and month > 10:
                    continue
                break

            num_days = calendar.monthrange(year, month)[1]
            valid_days = [day for day in range(1, num_days + 1)
                          if datetime(year, month, day).weekday() != 6]  # no Sundays
            order_day = random.choice(valid_days)
            order_date = datetime(year, month, order_day)

            # Order timestamps
            hour = random.randint(7, 23)
            minute = random.randint(0, 59)
            order_placed_ts = datetime(year, month, order_day, hour, minute)

            order_quantity = random.randint(5, 72)
            order_amount = random.randint(25, 200)

            order_assigned_ts = order_placed_ts + timedelta(minutes=random.randint(1, 5))
            picker_start_ts = order_assigned_ts + timedelta(minutes=random.randint(1, 10))
            min_end = 5 + int(order_quantity / 6)
            picker_end_ts = picker_start_ts + timedelta(minutes=random.randint(min_end, min_end + 10))
            expedited_delivery_ts = order_placed_ts + timedelta(hours=1)

            distance_km = round(random.random() * 11 + 1, 2)
            rider_pickup_ts = picker_end_ts + timedelta(minutes=random.randint(1, 12))
            min_drop = 10 + int((distance_km / 12) * 50)
            rider_drop_ts = rider_pickup_ts + timedelta(minutes=random.randint(min_drop, min_drop + 5))

            # Ensure rider drop within 07:15–23:30
            if not ((rider_drop_ts.hour > 7 or (rider_drop_ts.hour == 7 and rider_drop_ts.minute >= 15)) and
                    (rider_drop_ts.hour < 23 or (rider_drop_ts.hour == 23 and rider_drop_ts.minute <= 30))):
                continue

            picker = random.choice(pickers)
            rider = random.choice(riders)
            customer_id = random.choice(customers)
            issue_flag = random.choice(['Y', 'N', 'N', 'N', 'N'])

            rows.append({
                'Hub ID': hub['Hub ID'],
                'Order ID': order_id,
                'Customer ID': customer_id,
                'Picker ID': picker['Picker ID'],
                'Rider ID': rider['Rider ID'],
                'Order Date': order_date.strftime('%d-%m-%Y'),
                'Order Quantity': order_quantity,
                'Order Amount': order_amount,
                'Order Placed TS': order_placed_ts.strftime('%d-%m-%Y %H:%M'),
                'Order Assigned TS': order_assigned_ts.strftime('%d-%m-%Y %H:%M'),
                'Picker Start TS': picker_start_ts.strftime('%d-%m-%Y %H:%M'),
                'Picker End TS': picker_end_ts.strftime('%d-%m-%Y %H:%M'),
                'Expedited Delivery TS': expedited_delivery_ts.strftime('%d-%m-%Y %H:%M'),
                'Rider Pickup TS': rider_pickup_ts.strftime('%d-%m-%Y %H:%M'),
                'Rider Drop TS': rider_drop_ts.strftime('%d-%m-%Y %H:%M'),
                'Distance KM': distance_km,
                'Issue Flag': issue_flag,
                'City': hub['City'],
                'Hub Name': hub['Hub Name'],
                'Monthly Operating Cost': hub['Monthly Operating Cost'],
                'Picker Name': picker['Picker Name'],
                'Rider Name': rider['Rider Name']
            })

            order_id += 1

            # Write in chunks to avoid memory issues
            if len(rows) >= rows_per_chunk:
                df_chunk = pd.DataFrame(rows)
                df_chunk.to_csv(csv_file, mode='w' if first_chunk else 'a', index=False, header=first_chunk)
                first_chunk = False
                rows = []

        # Write remaining rows for this hub-month
        if rows:
            df_chunk = pd.DataFrame(rows)
            df_chunk.to_csv(csv_file, mode='w' if first_chunk else 'a', index=False, header=first_chunk)
            first_chunk = False
            rows = []

print(f"CSV file generated: {csv_file}")
