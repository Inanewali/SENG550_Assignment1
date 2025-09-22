import pandas as pd
import psycopg2

# -----------------------------
# Database Connection
# -----------------------------
conn = psycopg2.connect(
    dbname="storedb",
    user="postgres",
    password="ensf1234",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# -----------------------------
# Part 2: Load CSV Data
# -----------------------------
def load_csv_with_pandas(csv_file, table_name):
    df = pd.read_csv(csv_file)

    for _, row in df.iterrows():
        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(row))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        cur.execute(sql, tuple(row))

    conn.commit()
    print(f"Loaded {csv_file} into {table_name}")

# Load the three CSV files into tables
load_csv_with_pandas("customers.csv", "customers")
load_csv_with_pandas("orders.csv", "orders")
load_csv_with_pandas("deliveries.csv", "deliveries")

# -----------------------------
# Part 3: Add & Update Functions
# -----------------------------
# 1. Add customer
def add_customer(name, email, phone, address):
    cur.execute(
        """
        INSERT INTO customers (name, email, phone, address)
        VALUES (%s, %s, %s, %s)
        RETURNING customer_id;
        """,
        (name, email, phone, address)
    )
    customer_id = cur.fetchone()[0]
    conn.commit()
    print(f"Added customer {name} with id {customer_id}")
    return customer_id

# 2. Add order
def add_order(customer_id, order_date, total_amount, product_id, product_category, product_name):
    cur.execute(
        """
        INSERT INTO orders (customer_id, order_date, total_amount, product_id, product_category, product_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING order_id;
        """,
        (customer_id, order_date, total_amount, product_id, product_category, product_name)
    )
    order_id = cur.fetchone()[0]
    conn.commit()
    print(f"Added order {order_id} for customer {customer_id}")
    return order_id

# 3. Add delivery
def add_delivery(order_id, delivery_date, status):
    cur.execute(
        """
        INSERT INTO deliveries (order_id, delivery_date, status)
        VALUES (%s, %s, %s)
        RETURNING delivery_id;
        """,
        (order_id, delivery_date, status)
    )
    delivery_id = cur.fetchone()[0]
    conn.commit()
    print(f"Added delivery {delivery_id} for order {order_id}")
    return delivery_id

# 4. Update delivery status
def update_delivery_status(delivery_id, new_status):
    cur.execute(
        """
        UPDATE deliveries
        SET status = %s
        WHERE delivery_id = %s;
        """,
        (new_status, delivery_id)
    )
    conn.commit()
    print(f"Updated delivery {delivery_id} status to {new_status}")

# -----------------------------
# Adding Assignment Steps
# -----------------------------
if __name__ == "__main__":
    # Step 1: Add Liam Nelson
    liam_id = add_customer("Liam Nelson", "liam.nelson@example.com", "555-2468", "111 Elm Street")

    # Step 2: Add order for Liam
    liam_order_id = add_order(liam_id, "2025-06-01", 180.00, 116, "Electronics", "Bluetooth Speaker")

    # Step 3: Add delivery for Liam’s order
    liam_delivery_id = add_delivery(liam_order_id, "2025-06-03", "Pending")

    # Step 4: Update Liam’s delivery to Shipped
    update_delivery_status(liam_delivery_id, "Shipped")

    # Step 5: Add another customer, order, and delivery
    alex_id = add_customer("Alex Morgan", "alex.morgan@example.com", "555-7890", "222 Oak Street")
    alex_order_id = add_order(alex_id, "2025-07-10", 250.50, 202, "Home Appliances", "Air Fryer")
    alex_delivery_id = add_delivery(alex_order_id, "2025-07-12", "Pending")

    # Step 6: Update delivery_id = 3 to Delivered
    update_delivery_status(3, "Delivered")

# -----------------------------
# Close connection
# -----------------------------
cur.close()
conn.close()
