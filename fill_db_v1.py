import psycopg2
import random
from psycopg2 import sql
from datetime import datetime
import time

def get_next_table_name(table_queries, table_queries_iterator, table_name):
    try:
        next_table_name = next(table_queries_iterator)
    except StopIteration:
        return None  # If there are no more items, return None

    table_name = next_table_name
    insert_query = table_queries[table_name]
    return table_name, insert_query


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

num_insertions = int(input("Enter the number of insertions: "))


# Generate and execute the SQL insert statements for the "player," "item," "transaction," and "offer" tables
table_queries = {
    'category': "INSERT INTO flea_mktv.category (category_name, description) VALUES (%s, %s);",
    'tax': "INSERT INTO flea_mktv.tax (tax_rate) VALUES (%s);",
    'product': "INSERT INTO flea_mktv.product (category_id, item_name, item_condition, weight, base_price) VALUES (%s, %s, %s, %s, %s);",
    'player': "INSERT INTO flea_mktv.player (name, email, join_date) VALUES (%s, %s, %s);",
    'item': "INSERT INTO flea_mktv.item (product_id, player_id, quantity) VALUES (%s, %s, %s);",
    'offer': "INSERT INTO flea_mktv.offer (seller_id, tax_id, offer_status, product_id, quantity) VALUES (%s, %s, %s, %s, %s);",
    'transaction': """INSERT INTO flea_mktv."transaction" (buyer_id, offer_id, quantity, "time") VALUES (%s, %s, %s, %s);"""
}


used_item_ids = []  # List to keep track of used item_ids
start_time = time.time()  # Start time
insertion_count = 0
table_queries_iterator = iter(table_queries)
for table_name in table_queries_iterator:
    insert_query = table_queries[table_name]

    while insertion_count < num_insertions:
        if table_name == 'category':
            # Insert categories
            category_names = ['Category1', 'Category2', 'Category3', 'Category4', 'Category5',
                                            'Category6', 'Category7', 'Category8', 'Category9', 'Category10']
            for category_name in category_names:
                description = "Description for " + category_name

                values = (category_name, description)
                cursor.execute(insert_query, values)

                insertion_count += 1
            result = get_next_table_name(table_queries, table_queries_iterator, table_name)
            if result is not None:
                next_table_name, insert_query = result
                table_name=next_table_name
            

        elif table_name == 'tax':
            # Insert taxes
            tax_values = [(0.1,), (0.15,), (0.2,), (0.25,), (0.3,),
                        (0.35,), (0.4,), (0.45,), (0.5,), (0.55,)]

            for tax_value in tax_values:
                values = tax_value
                cursor.execute(insert_query, values)

                insertion_count += 1
            result = get_next_table_name(table_queries, table_queries_iterator, table_name)
            if result is not None:
                next_table_name, insert_query = result
                table_name=next_table_name
        

        elif table_name == 'product':
            # Insert products
            for category_id in range(1, 11):
                for _ in range(10):
                    item_name = "Product" + str(category_id) + str(_)
                    item_condition = round(random.uniform(1.0, 5.0), 2)
                    weight = random.randint(1, 10)
                    base_price = round(random.uniform(10.0, 100.0), 2)

                    values = (category_id, item_name, item_condition, weight, base_price)
                    cursor.execute(insert_query, values)

                    insertion_count += 1
            result = get_next_table_name(table_queries, table_queries_iterator, table_name)
            if result is not None:
                next_table_name, insert_query = result
                table_name=next_table_name

        # Generate data based on the table
        elif table_name == 'player':
            # Generate player data
            name = f"User{insertion_count + 1}"
            email = f"user{insertion_count + 1}@example.com"
            join_date = datetime.now().strftime('%Y-%m-%d')
            values = (name, email, join_date)
            cursor.execute(insert_query, values)

            insertion_count += 1
            result = get_next_table_name(table_queries, table_queries_iterator, table_name)
            if result is not None:
                next_table_name, insert_query = result
                table_name=next_table_name

        elif table_name == 'item':
            # Retrieve existing product_ids and player_ids
            cursor.execute("SELECT product_id FROM flea_mktv.product;")
            product_ids = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT player_id FROM flea_mktv.player;")
            player_ids = [row[0] for row in cursor.fetchall()]

            # Generate item data
            product_id = random.choice(product_ids)
            player_id = random.choice(player_ids)
            quantity = random.randint(1, 10)
            values = (product_id, player_id, quantity)
            cursor.execute(insert_query, values)

            insertion_count += 1
            result = get_next_table_name(table_queries, table_queries_iterator, table_name)
            if result is not None:
                next_table_name, insert_query = result
                table_name=next_table_name

        elif table_name == 'offer':
            # Retrieve existing player_ids, tax_ids, and product_ids
            cursor.execute("SELECT player_id FROM flea_mktv.player;")
            player_ids = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT tax_id FROM flea_mktv.tax;")
            tax_ids = [row[0] for row in cursor.fetchall()]

            cursor.execute("SELECT product_id FROM flea_mktv.product;")
            product_ids = [row[0] for row in cursor.fetchall()]
            # Generate offer data
            seller_id = random.choice(player_ids)
            tax_id = random.choice(tax_ids)
            offer_status = random.choice(['Active', 'Bought', 'Expired'])

            # Retrieve the items for the selected seller
            seller_item_query = sql.SQL("SELECT item_id, quantity FROM flea_mktv.item WHERE player_id = %s;")
            cursor.execute(seller_item_query, (seller_id,))
            seller_items = cursor.fetchall()

            # Choose a random item from the seller's items
            if not seller_items:
                continue  # Skip the current iteration if the seller has no items
            item_id, item_quantity = random.choice(seller_items)

            # Set the offer quantity equal to the item quantity
            quantity = item_quantity

            values = (seller_id, tax_id, offer_status, item_id, quantity)
            cursor.execute(insert_query, values)

            insertion_count += 1
            result = get_next_table_name(table_queries, table_queries_iterator, table_name)
            if result is not None:
                next_table_name, insert_query = result
                table_name=next_table_name


        elif table_name == 'transaction':
            # Generate transaction data
            offer_id = random.randint(1, num_insertions)
            quantity = random.randint(1, 10)
            time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Retrieve the seller_id from the offer
            offer_seller_query = sql.SQL("SELECT seller_id FROM flea_mktv.offer WHERE offer_id = %s;")
            cursor.execute(offer_seller_query, (offer_id,))
            seller_row = cursor.fetchone()

            if seller_row is not None:
                seller_id = seller_row[0]
                # Rest of your code for generating transaction data
            else:
                # Handle the case when seller_id is not found
                # This could be a situation where the offer_id doesn't exist or the corresponding offer doesn't have a seller_id
                print("Error: Seller ID not found for offer ID:", offer_id)

            # Generate a buyer_id that is not the same as the seller_id
            buyer_ids = list(range(1, num_insertions + 1))
            buyer_ids.remove(seller_id)
            buyer_id = random.choice(buyer_ids)

            # Check if the quantity is valid based on the offer
            offer_query = sql.SQL("SELECT quantity FROM flea_mktv.offer WHERE offer_id = %s;")
            cursor.execute(offer_query, (offer_id,))
            offer_quantity = cursor.fetchone()[0]

            if quantity <= offer_quantity:
                values = (buyer_id, offer_id, quantity, time_now)
                cursor.execute(insert_query, values)

                insertion_count += 1
            result = get_next_table_name(table_queries, table_queries_iterator, table_name)
            if result is not None:
                next_table_name, insert_query = result
                table_name=next_table_name

end_time = time.time()  # End time
execution_time = end_time - start_time  # Calculate the execution time in seconds

print(f"Insertion into flea_mktv completed. {insertion_count} records inserted in {execution_time} seconds.")

# Commit the changes and close the cursor and connection
connection.commit()
cursor.close()
connection.close()
