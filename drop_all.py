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

# Reset each sequence to start from 1
sequences = [
    "flea_mktv.category_categories_id_seq",
    "flea_mktv.item_item_id_seq2",
    "flea_mktv.offer_offer_id_seq2",
    "flea_mktv.player_player_id_seq2",
    "flea_mktv.product_product_id_seq2",
    "flea_mktv.tax_tax_id_seq2",
    "flea_mktv.transaction_transaction_id_seq2"
]

for sequence in sequences:
    cursor.execute(f"ALTER SEQUENCE {sequence} RESTART WITH 1;")

# Enable foreign key checks again
cursor.execute('SET session_replication_role = DEFAULT;')

# Commit the changes and close the cursor and connection
connection.commit()
cursor.close()
connection.close()

print("All data has been dropped from the database.")
