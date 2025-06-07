import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# 1. Customers
num_customers = 100
customers = pd.DataFrame({
    "customer_id": [f"CUST{i:03d}" for i in range(1, num_customers + 1)],
    "name": [fake.name() for _ in range(num_customers)],
    "email": [fake.email() for _ in range(num_customers)],
    "join_date": [fake.date_between(start_date='-2y', end_date='today') for _ in range(num_customers)]
})

# 2. Products
num_products = 10
products = pd.DataFrame({
    "product_id": [f"PROD{i:02d}" for i in range(1, num_products + 1)],
    "name": [fake.word().capitalize() for _ in range(num_products)],
    "price": [round(random.uniform(10, 500), 2) for _ in range(num_products)],
    "category": [fake.word() for _ in range(num_products)]
})

# 3. Orders
num_orders = 1000
start_date = datetime.now() - timedelta(days=30)
end_date = datetime.now()

orders = pd.DataFrame({
    "order_id": [f"ORD{i:04d}" for i in range(1, num_orders + 1)],
    "order_date": [fake.date_between(start_date=start_date, end_date=end_date) for _ in range(num_orders)],
    "customer_id": [random.choice(customers['customer_id']) for _ in range(num_orders)],
    "product_id": [random.choice(products['product_id']) for _ in range(num_orders)],
    "quantity": [random.randint(1, 5) for _ in range(num_orders)]
})

orders = orders.merge(products[['product_id', 'price']], on='product_id')
orders['total_amount'] = orders['price'] * orders['quantity']
orders = orders.drop(columns=['price'])

spark.createDataFrame(customers).write.mode("overwrite").saveAsTable("demo.customers")
spark.createDataFrame(products).write.mode("overwrite").saveAsTable("demo.products")
spark.createDataFrame(orders).write.mode("overwrite").saveAsTable("demo.orders")



customers.head(), products.head(), orders.head()
