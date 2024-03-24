import boto3
import random
import time
from decimal import Decimal

# Initialize DynamoDB resource
session = boto3.Session(profile_name='default', region_name='us-east-1')
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('ordersFact')


# generate mock orders data
def generate_orders_data():
    # Generate some random orders data
    order_id = str(random.randint(1000, 100000))
    product_name = random.choice(
        ['Phone', 'Tablet', 'Headphones', 'Charger', 'Electronics', 'Laptop', 'Keyboard', 'Monitor', 'Mouse'])
    quantity = random.randint(1, 8)
    price = Decimal(str(round(random.uniform(10.0, 500.0), 2)))
    timestamp = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    return {
        'order_id': order_id,
        'product_name': product_name,
        'quantity': quantity,
        'price': price,
        'timestamp': timestamp
    }


# Insert data dynamodb
def insert_into_dynamodb(orders_data):
    # Insert data into DynamoDB
    try:
        table.put_item(Item=orders_data)
        print(f"Inserted data: {orders_data}")
    except Exception as e:
        print(f"Error inserting data: {str(e)}")


if __name__ == '__main__':
    try:
        while True:
            orders_data = generate_orders_data()
            insert_into_dynamodb(orders_data)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nScript stopped by manual intervention!")
