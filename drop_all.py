import psycopg2

# Establish a connection
connection = psycopg2.connect(
    host='localhost',
    port='5432',
    user='postgres',
    password='12345',
    database='postgres'
)

# Create a cursor to execute SQL statements
cursor = connection.cursor()

# List of table names
table_names = ['category', 'tax', 'product', 'player', 'item', 'offer', 'transaction']

# Disable foreign key checks temporarily
cursor.execute('SET session_replication_role = replica;')

# Drop all data from each table
for table_name in table_names:
    cursor.execute(f'TRUNCATE TABLE flea_mktv.{table_name} RESTART IDENTITY CASCADE;')

# Enable foreign key checks again
cursor.execute('SET session_replication_role = DEFAULT;')

# Commit the changes and close the cursor and connection
connection.commit()
cursor.close()
connection.close()

print("All data has been dropped from the database.")
