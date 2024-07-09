from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra import WriteFailure
import random
from datetime import datetime, timedelta
import time

# Define SSL context
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.load_verify_locations('path/to/sf-class2-root.crt')  # Update this path
ssl_context.verify_mode = CERT_REQUIRED

# Define authentication provider
auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')

# Connect to the cluster
cluster = Cluster(['your_amazon_endpoint'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
session = cluster.connect('your_keyspace_name')

# Prepare the insert statement
insert_query = """
INSERT INTO your_table_name(
    billing_year, billing_month, billing_date, device_serial_number, billing_datetime,
    average_power_factor_for_billing_period, billing_power_off_duration_in_billing, billing_power_on_duration_in_billing,
    cumulative_energy_kvah_export, cumulative_energy_kvah_import, cumulative_energy_kvah_tier1, cumulative_energy_kvah_tier2,
    cumulative_energy_kvah_tier3, cumulative_energy_kvah_tier4, cumulative_energy_kvah_tier5, cumulative_energy_kvah_tier6,
    cumulative_energy_kvah_tier7, cumulative_energy_kvah_tier8, cumulative_energy_kwh_export, cumulative_energy_kwh_import,
    cumulative_energy_kwh_tier1, cumulative_energy_kwh_tier2, cumulative_energy_kwh_tier3, cumulative_energy_kwh_tier4,
    cumulative_energy_kwh_tier5, cumulative_energy_kwh_tier6, cumulative_energy_kwh_tier7, cumulative_energy_kwh_tier8,
    dcu_serial_number, dt_name, feeder_name, maximum_demand_kva, maximum_demand_kva_datetime, maximum_demand_kw,
    maximum_demand_kw_datetime, mdas_datetime, meter_datetime, owner_name, subdevision_name, substation_name)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
prepared = session.prepare(insert_query)
prepared.consistency_level = ConsistencyLevel.LOCAL_QUORUM

# Function to generate random data
def generate_random_data():
    billing_year = 2022
    billing_month = random.randint(1, 12)
    billing_date = random.randint(1, 28)  # Simplified for all months
    device_serial_number = f'{random.randint(10000000, 99999999)}'
    billing_datetime = datetime(billing_year, billing_month, billing_date, random.randint(0, 23), random.randint(0, 59))
    data = (
        billing_year, billing_month, billing_date, device_serial_number, billing_datetime,
        random.uniform(0.8, 1.0), random.uniform(0, 1000), random.uniform(0, 1000),
        random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000),
        random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000),
        random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000),
        random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000),
        random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000), random.uniform(0, 10000),
        '', 'DT_NAME', 'FEEDER_NAME', random.uniform(0, 10), billing_datetime, random.uniform(0, 10),
        billing_datetime, datetime.now(), datetime.now(), 'OWNER_NAME', 'SUBDIVISION_NAME', 'SUBSTATION_NAME'
    )
    return data

# Insert data in a loop with retry mechanism
num_records = 1000000
retries = 3

for i in range(1, num_records + 1):
    data = generate_random_data()
    for attempt in range(retries):
        try:
            session.execute(prepared, data)
            print(f'Inserted {i} successfully...')
            break
        except WriteFailure as e:
            print(f'WriteFailure on record {i}, attempt {attempt + 1}: {e}')
            time.sleep(1)  # Wait before retrying
        except Exception as e:
            print(f'Error on record {i}, attempt {attempt + 1}: {e}')
            time.sleep(1)  # Wait before retrying
    else:
        print(f'Failed to insert record {i} after {retries} attempts')

# Close the session and cluster
session.shutdown()
cluster.shutdown()
