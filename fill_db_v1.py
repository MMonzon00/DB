import psycopg2
import random
from psycopg2 import sql
from datetime import datetime
import time

def insertarEnTabla(tableName):
    # Generate and execute the SQL insert statements for the "player," "item," "transaction," and "offer" tables
    table_queries = {
    'category': "INSERT INTO flea_mktv.category (category_name, description) VALUES (%s, %s);",
    'tax': "INSERT INTO flea_mktv.tax (tax_rate) VALUES (%s);",
    'player': "INSERT INTO flea_mktv.player (name, email, join_date) VALUES (%s, %s, %s);",
    'product': "INSERT INTO flea_mktv.product (category_id, item_name, item_condition, weight, base_price) VALUES (%s, %s, %s, %s, %s);",
    'item': "INSERT INTO flea_mktv.item (product_id, player_id, quantity) VALUES (%s, %s, %s);",
    'offer': "INSERT INTO flea_mktv.offer (seller_id, tax_id, offer_status, product_id, quantity) VALUES (%s, %s, %s, %s, %s);",
    'transaction': """INSERT INTO flea_mktv."transaction" (buyer_id, offer_id, quantity, "time") VALUES (%s, %s, %s, %s);"""
    }


    used_item_ids = []  # List to keep track of used item_ids
    start_time = time.time()  # Start time
    insertion_count = 0
    table_queries_iterator = iter(table_queries)
    for table_name in table_queries_iterator:
        table_name = tableName
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
                    connection.commit()
                    insertion_count += 1
            elif table_name == 'tax':
                # Insert taxes
                tax_values = [(0.1,), (0.15,), (0.2,), (0.25,), (0.3,),
                            (0.35,), (0.4,), (0.45,), (0.5,), (0.55,)]

                for tax_value in tax_values:
                    values = tax_value
                    cursor.execute(insert_query, values)
                    connection.commit()
                    insertion_count += 1
            

            if table_name == 'product':
                insert_query='INSERT INTO flea_mktv.product (category_id, item_name, item_condition, weight, base_price) VALUES (%s, %s, %s, %s, %s);'
                # Insert products
                for category_id in range(1, 11):
                    for _ in range(10):
                        item_name = "Product" + str(category_id) + str(_)
                        item_condition = round(random.uniform(1.0, 5.0), 2)
                        weight = random.randint(1, 10)
                        base_price = round(random.uniform(10.0, 100.0), 2)

                        values = (category_id, item_name, item_condition, weight, base_price)
                        cursor.execute(insert_query, values)
                        connection.commit()
                        insertion_count += 1

            # Generate data based on the table
            elif table_name == 'player':
                # Generate player data
                name = f"User{insertion_count + 1}"
                email = f"user{insertion_count + 1}@example.com"
                join_date = datetime.now().strftime('%Y-%m-%d')
                values = (name, email, join_date)
                cursor.execute(insert_query, values)
                connection.commit()
                insertion_count += 1

            if table_name == 'item':
                insert_query = 'INSERT INTO flea_mktv.item (product_id, player_id, quantity) VALUES (%s, %s, %s);'
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
                connection.commit()
                insertion_count += 1

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

                cursor.callproc("flea_mktv.get_seller_reputation", [seller_id])
                rep_score = float(cursor.fetchone()[0])

                cursor.execute('select count(o.seller_id) from flea_mktv.offer o where o.seller_id = %s',(seller_id,))
                offers_seller = int(cursor.fetchone()[0])
                if rep_score >= -10 and rep_score <= 0.19 and offers_seller >= 1:
                    continue
                elif rep_score >= 0.2 and rep_score <= 6.99 and offers_seller >= 2:
                    continue
                elif rep_score >= 7 and rep_score <= 29.99 and offers_seller >= 3:
                    continue
                elif rep_score >= 30 and rep_score <= 59.99 and offers_seller >= 4:
                    continue
                elif rep_score >= 60 and rep_score <= 99.99 and offers_seller >= 5:
                    continue
                elif rep_score >= 100 and rep_score <= 149.99 and offers_seller >= 6:
                    continue
                elif rep_score >= 150 and rep_score <= 999.99 and offers_seller >= 8:
                    continue
                elif rep_score >= 1000 and offers_seller >= 10:
                    continue
                # Retrieve the items for the selected seller
                seller_item_query = sql.SQL("SELECT product_id, offerable_quantity FROM flea_mktv.view_offerable_items WHERE player_id = %s;")
                cursor.execute(seller_item_query, (seller_id,))
                seller_products = cursor.fetchall()
                
                # Choose a random item from the seller's items
                if not seller_products:
                    continue  # Skip the current iteration if the seller has no items
                product_id, offerable_quantity = random.choice(seller_products)
                if offerable_quantity <= 0:
                    continue
                quantity = random.randint(1,int(offerable_quantity))
                1==1
                values = (seller_id, tax_id, offer_status, product_id, quantity)
                cursor.execute(insert_query, values)
                connection.commit()
                insertion_count += 1


            elif table_name == 'transaction':
                
                # Generate transaction data
                cursor.execute("SELECT offer_id FROM flea_mktv.offer;")
                offer_ids = [row[0] for row in cursor.fetchall()]
                offer_id = random.choice(offer_ids)
                quantity = random.randint(1, 10)
                time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Retrieve the seller_id from the offer
                offer_seller_query = sql.SQL("SELECT seller_id FROM flea_mktv.offer WHERE offer_id = %s;")
                cursor.execute(offer_seller_query, (offer_id,))
                
                seller_id = cursor.fetchone()[0]
                cursor.execute("SELECT COALESCE(SUM(quantity), 0) as transacted_quantity FROM flea_mktv.\"transaction\"WHERE offer_id = %s",(offer_id,))
                transacted_quantity = int(cursor.fetchone()[0])
                # Generate a buyer_id that is not the same as the seller_id
                buyer_ids = list(range(1, num_insertions + 1))
                buyer_ids.remove(seller_id)
                buyer_id = random.choice(buyer_ids)

                # Check if the quantity is valid based on the offer
                offer_query = sql.SQL("SELECT quantity FROM flea_mktv.offer WHERE offer_id = %s;")
                cursor.execute(offer_query, (offer_id,))
                offer_quantity = cursor.fetchone()[0]

                if quantity + transacted_quantity <= offer_quantity:
                    values = (buyer_id, offer_id, quantity, time_now)
                    cursor.execute(insert_query, values)
                    connection.commit()
                    insertion_count += 1
    end_time = time.time()  # End time
    execution_time = end_time - start_time  # Calculate the execution time in seconds
    print(f"Insertion into flea_mktv completed. {insertion_count} records inserted in {execution_time} seconds.")

while True:
        # Establish a connection
    connection = psycopg2.connect(
        host='localhost',
        port='5432',
        user='postgres',
        password='123',
        database='postgres'
    )

    # Create a cursor to execute SQL statements
    cursor = connection.cursor()
    num_insertions = int(input("Enter the number of insertions: "))
    insertarEnTabla(input('Inserte el nombre de la tabla: '))

    # Commit the changes and close the cursor and connection
    connection.commit()
    cursor.close()
    connection.close()
