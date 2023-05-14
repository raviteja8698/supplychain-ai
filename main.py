import logging
import os
import smtplib
import time
from decimal import Decimal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler
from psycopg2.extras import DictCursor

DB_NAME = "Product"
DB_USER = "postgres"
DB_PASS = "Ravi@8698"
DB_HOST = "localhost"

MY_ADDRESS = os.getenv('MY_ADDRESS')
PASSWORD = os.getenv('PASSWORD')
SMTP_SERVER = os.getenv('SMTP_SERVER')


def send_email(subject, message, recipient):
    try:
        s = smtplib.SMTP(host=SMTP_SERVER, port=587)
        s.starttls()
        s.login(MY_ADDRESS, PASSWORD)
        msg = MIMEMultipart()
        msg['From'] = MY_ADDRESS
        msg['To'] = recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))
        s.send_message(msg)
        del msg
        s.quit()
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


# ... rest of the code ...


def notify_low_stock(product_id, min_stock_level):
    # Notify via email when a product's stock level is low
    product = get_product(product_id)
    if product is None:
        print(f"No product found with id {product_id}")
        return

    if product['availablestock'] < min_stock_level:
        subject = f"Low Stock Alert: {product['productname']}"
        message = f"The stock level of {product['productname']} is below the minimum threshold. Current stock: {product['availablestock']}."
        recipient = "your-email@example.com"  # Replace with the actual recipient
        send_email(subject, message, recipient)


def check_all_products_stock():
    all_products = get_all_products()
    min_stock_level = 10  # This can be adjusted as per your business requirements

    for product in all_products:
        if product[
            'availablestock'] < min_stock_level:  # Assuming 'availablestock' is the attribute name for the stock level of a product
            notify_low_stock(product['productid'], min_stock_level)


def get_all_products():
    with get_db_connection() as conn:
        with get_db_cursor(conn) as cur:
            cur.execute("SELECT * FROM Product")
            return cur.fetchall()


# Create an instance of scheduler
scheduler = BackgroundScheduler()
# Add a job to the scheduler
scheduler.add_job(check_all_products_stock, 'interval', hours=1)
# Start the scheduler
scheduler.start()


def get_db_connection():
    try:
        return psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST
        )
    except psycopg2.OperationalError as e:
        print(f"Could not connect to database: {e}")
        return None


def get_db_cursor(conn):
    return conn.cursor(cursor_factory=DictCursor)


def setup_db():
    # Setup the database with necessary tables
    conn = get_db_connection()

    if conn is not None:
        with conn:
            with get_db_cursor(conn) as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS Product (
                        productid serial PRIMARY KEY,
                        productname varchar(255),
                        productdescription text,
                        supplierid int,
                        category varchar(255),
                        price numeric,
                        cost numeric,
                        sku varchar(255),
                        barcode varchar(255),
                        availablestock int,
                        minimumstocklevel int,
                        leadtime int,
                        productimage varchar(255),
                        productsize varchar(255),
                        productweight numeric,
                        productdimensions varchar(255),
                        reorderquantity int,
                        reorderthreshold int,
                        manufacturingdate date,
                        expirationdate date,
                        location varchar(255),
                        manufacturer varchar(255),
                        safetystocklevel int
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ProductHistory (
                        historyid serial PRIMARY KEY,
                        productid int,
                        updatetime timestamp default current_timestamp,
                        productname varchar(255),
                        price numeric
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS OrderHistory (
                        orderid serial PRIMARY KEY,
                        productid int,
                        quantity int,
                        reorderdate timestamp default current_timestamp
                    )
                """)
                conn.commit()


def validate_product(product):
    # Validate product details before insertion or update
    required_fields = ['productname', 'price', 'reorderquantity', 'reorderthreshold']

    for field in required_fields:
        if field not in product:
            raise ValueError(f"{field} is required.")
        if field == 'productname' and not isinstance(product[field], str):
            raise TypeError("Product name must be a string.")
        if field in ['price', 'reorderquantity', 'reorderthreshold'] and not isinstance(product[field],
                                                                                        (int, float, Decimal)):
            raise TypeError(f"{field} must be a number.")


def validate_connection(conn):
    if conn is None:
        logging.error("Could not connect to the database. Exiting.")
        return False
    return True


def create_valid_product(product):
    # Ensure that all required fields are included in the product
    default_product = {
        'productname': 'Default product',
        'price': 0,
        'reorderquantity': 0,  # Added this line
        'reorderthreshold': 0  # Added this line
    }
    # Update the default product with the provided product values
    default_product.update(product)
    return default_product


def get_product(product_id):
    # Get a product from the database by ID
    with get_db_connection() as conn:
        with get_db_cursor(conn) as cur:
            cur.execute("SELECT * FROM Product WHERE productid = %s", (product_id,))
            product = cur.fetchone()
            if product is None:
                raise ValueError(f"Product with id {product_id} not found.")
            return product


def update_product(product_id, product):
    conn = get_db_connection()
    if not validate_connection(conn):
        return
    # Update a product's details in the database
    validate_product(product)
    conn = get_db_connection()
    if conn is not None:
        with conn:
            with get_db_cursor(conn) as cur:
                fields = product.keys()
                values = [product[field] for field in fields]
                update = sql.SQL('UPDATE Product SET ({}) = ({}) WHERE productid = {}').format(
                    sql.SQL(',').join(map(sql.Identifier, fields)),
                    sql.SQL(',').join(sql.Placeholder() * len(fields)),
                    sql.Placeholder()
                )
                cur.execute(update, values + [product_id])
                insert_history = sql.SQL('INSERT INTO ProductHistory (productid, {}) VALUES ({})').format(
                    sql.SQL(',').join(map(sql.Identifier, fields)),
                    sql.SQL(',').join(sql.Placeholder() * (len(fields) + 1))
                )
                cur.execute(insert_history, [product_id] + values)
                conn.commit()


def check_stock_levels():
    conn = get_db_connection()
    if not validate_connection(conn):
        return
    # Check the stock levels of all products and reorder or adjust as necessary
    conn = get_db_connection()
    if conn is not None:
        with conn:
            with get_db_cursor(conn) as cur:
                cur.execute("SELECT * FROM Product")
                products = cur.fetchall()
                for product in products:
                    if product['availablestock'] <= product['reorderthreshold']:
                        reorder_product(product['productid'], product['reorderquantity'])
                    elif 'maximumstocklevel' in product and product['availablestock'] > product['maximumstocklevel']:
                        adjust_stock_level(product['productid'], product['maximumstocklevel'])
                conn.commit()



def adjust_stock_level(product_id, maximum_stock_level):
    conn = get_db_connection()
    if not validate_connection(conn):
        return

    # Adjust a product's stock level to the maximum
    def adjust_stock_level(product_id):
        product = get_product(product_id)
        # Check if the product's available stock is above the maximum stock level
        if product['availablestock'] > product['maximumstocklevel']:
            # Adjust the stock level
            update_product(product_id, {'availablestock': product['maximumstocklevel']})

            conn = get_db_connection()
            if conn is not None:
                with conn:
                    with get_db_cursor(conn) as cur:
                        cur.execute("UPDATE Product SET availablestock = %s WHERE productid = %s",
                                    (maximum_stock_level, product_id))
                        conn.commit()


def update_price_based_on_cost(product_id, new_cost, profit_margin):
    # Calculate the new price based on the updated cost and the desired profit margin
    new_price = new_cost * (1 + profit_margin)
    # Update the product's price in the database
    update_product(product_id, {'price': new_price})


def apply_seasonal_discount(product_id, discount):
    # Get the current product price
    product = get_product(product_id)
    current_price = product['price']
    # Calculate the discounted price
    discounted_price = current_price * (1 - discount)
    # Update the product's price in the database
    update_product(product_id, {'price': discounted_price})


def notify_reorder(product_id, quantity):
    product = get_product(product_id)
    if product is None:
        print(f"No product found with id {product_id}")
        return

    if 'reorderquantity' not in product:
        product['reorderquantity'] = quantity  # Add the 'reorderquantity' key to the product dictionary

    if 'reorderthreshold' not in product:
        product['reorderthreshold'] = product[
            'reorderquantity']  # Add the 'reorderthreshold' key to the product dictionary

    update_product(product_id, product)

    # ... rest of the code ...


def discontinue_product(product_id):
    # Mark a product as discontinued
    update_product(product_id, {'discontinued': True})


def reorder_product(product_id, quantity):
    product = get_product(product_id)
    # Check if the product is discontinued before reordering
    if 'discontinued' in product and product['discontinued']:
        print(f"Product {product_id} is discontinued. Not reordering.")
        return

    conn = get_db_connection()
    if not validate_connection(conn):
        return

    # Reorder a product when its stock is low
    if product['availablestock'] < product['minimumstocklevel']:
        # Calculate the quantity to reorder based on the lead time
        quantity_to_reorder = quantity + product[
            'leadtime'] * AVERAGE_DAILY_SALES  # Assuming AVERAGE_DAILY_SALES is a constant that represents the average daily sales of a product

        if conn is not None:
            with conn:
                with get_db_cursor(conn) as cur:
                    cur.execute("UPDATE Product SET availablestock = availablestock + %s WHERE productid = %s",
                                (quantity_to_reorder, product_id))
                    insert_order_history = sql.SQL(
                        'INSERT INTO OrderHistory (productid, quantity, reorderdate) VALUES (%s, %s, NOW())')
                    cur.execute(insert_order_history, [product_id, quantity_to_reorder])
                    conn.commit()

                    # Notify about the reordering
                    notify_reorder(product_id, quantity_to_reorder)


def apply_discount(product_id, discount):
    # Apply a discount to a product, updating its price
    try:
        product = get_product(product_id)
        if product is None:
            print(f"No product found with id {product_id}")
            return
        new_price = product['price'] * Decimal(1 - discount)  # Convert float to Decimal before operation
        update_product(product_id, {'productname': product['productname'], 'price': new_price,
                                    'reorderquantity': product['reorderquantity']})

        # Notify about the discount
        subject = f"Discount applied for {product['productname']}"
        message = f"A discount of {discount * 100}% has been applied to {product['productname']}. The new price is ${new_price}."
        recipient = "your-email@example.com"  # Replace with the actual recipient
        send_email(subject, message, recipient)
    except ValueError as e:
        print(e)
        return


def analyze_product_profitability(product_id):
    product = get_product(product_id)
    # Calculate the profit for the product (revenue - cost)
    profit = product['price'] - product['cost']
    # Flag products with low profitability
    if profit < LOW_PROFIT_THRESHOLD:
        print(f"Product {product_id} has low profitability: {profit}")


def main():
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    setup_db()
    logging.info("Database setup completed")

    product = create_valid_product({
        'productname': 'Example Product 11',
        'productdescription': 'This is an example product 10',
        'supplierid': 1,
        'category': 'Electronics',
        'price': 19.99,
        'cost': 10.00,
        'sku': 'EX123',
        'barcode': '123456789012',
        'availablestock': 200,
        'minimumstocklevel': 20,
        'leadtime': 7,
        'productimage': 'http://example.com/product.jpg',
        'productsize': 'Medium',
        'productweight': 0.5,
        'productdimensions': '10x10x10',
        'reorderquantity': 100,
        'reorderthreshold': 10,
        'manufacturingdate': '2023-01-01',
        'expirationdate': '2025-01-01',
        'location': 'Warehouse 1',
        'manufacturer': 'Example Manufacturer',
        'safetystocklevel': 30
    })

    conn = get_db_connection()
    if conn is None:
        logging.error("Could not connect to the database. Exiting.")
        return

    create_valid_product(product)

    logging.info(f"Product {product['productname']} created")

    apply_discount(1, 0.1)
    logging.info(f"Discount applied to product 1")

    scheduler = BackgroundScheduler()
    scheduler.add_job(check_stock_levels, 'interval', hours=1)
    scheduler.start()

    while True:
        logging.info("Checking stock levels...")
        check_stock_levels()
        logging.info("Stock levels checked, sleeping for 60 seconds")
        time.sleep(60)


def main():
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    setup_db()
    logging.info("Database setup completed")

    product = create_valid_product({
        'productname': 'Product10',
        'productdescription': 'This is an example product 10',
        'supplierid': 1,
        'category': 'Electronics',
        'price': 19.99,
        'cost': 10.00,
        'sku': 'EX123',
        'barcode': '123456789012',
        'availablestock': 200,
        'minimumstocklevel': 20,
        'leadtime': 7,
        'productimage': 'http://example.com/product.jpg',
        'productsize': 'Medium',
        'productweight': 0.5,
        'productdimensions': '10x10x10',
        'reorderquantity': 100,
        'reorderthreshold': 10,
        'manufacturingdate': '2023-01-01',
        'expirationdate': '2025-01-01',
        'location': 'Warehouse 1',
        'manufacturer': 'Example Manufacturer',
        'safetystocklevel': 30
    })

    conn = get_db_connection()
    if conn is None:
        logging.error("Could not connect to the database. Exiting.")
        return

    create_valid_product(product)

    logging.info(f"Product {product['productname']} created")

    apply_discount(1, 0.1)
    logging.info(f"Discount applied to product 1")

    scheduler = BackgroundScheduler()
    scheduler.add_job(check_stock_levels, 'interval', hours=1)
    scheduler.start()

    while True:
        logging.info("Checking stock levels...")
        check_stock_levels()
        logging.info("Stock levels checked, sleeping for 60 seconds")
        time.sleep(60)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    main()
