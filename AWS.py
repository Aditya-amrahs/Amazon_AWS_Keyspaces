from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
from datetime import timedelta
import time
import os


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the relative path to the .crt file
crt_file = os.path.join(script_dir, 'sf-class2-root.crt')

# Define SSL context
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
ssl_context.load_verify_locations(crt_file)
ssl_context.verify_mode = CERT_REQUIRED

# Define authentication provider
# Add your AWS Keyspace service credentials in username and password which you can generate from IAM user console
auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')

# Connect to the cluster
# Add the amazon endpoint for your region
cluster = Cluster(['your_amazon_endpoint'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
session = cluster.connect()

# Function to format timedelta including milliseconds
def format_timedelta(seconds):
    hours, rem = divmod(seconds, 3600)
    minutes, rem = divmod(rem, 60)
    seconds, milliseconds = divmod(rem, 1)
        
    milliseconds = int(milliseconds * 1000)
    elapsed_time_str = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}:{milliseconds:03}"
        
    return elapsed_time_str

# Function to execute queries
def execute_query():
    while True:
        query = input("\nEnter your CQL query (or type 'quit' to exit): ")
        if query.lower() == 'quit':
            print("Exiting the program...\n")
            break
        try:
            start_time = time.time()
            result = session.execute(query)
            row_count = 0
            rows = []
            for row in result:
                rows.append(row)
                row_count += 1
            
            print("\n")
            # Get column names from metadata
            column_names = result.column_names
            # Print column names
            print("\t".join(column_names))
            
            # Print rows
            for row in rows:
                print("\t".join(str(value) for value in row))
            end_time = time.time()
            elapsed_time = end_time - start_time
            formatted_elapsed_time = format_timedelta(elapsed_time)
            print(f"\nTotal rows: {row_count}")
            print(f"Elapsed time: {formatted_elapsed_time} (HH:MM:SS.mmm)")
        except Exception as e:
            print(f"An error occurred: {e}")

# Run the function
execute_query()

# Close the session and cluster connection
session.shutdown()
cluster.shutdown()
