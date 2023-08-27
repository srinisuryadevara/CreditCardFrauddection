from pymongo import MongoClient
import datetime
import statistics

# Connect to the MongoDB server
client = MongoClient('mongodb://localhost:27017/')

# Create or access the 'capstone_project' database
db = client['capstone_project']

# Load data from the Hadoop output or Hive table
# For this example, I'll assume you have a list of dictionaries with transaction data
data = [
    {
        "CARD_ID": "123",
        "AMOUNT": 100.0,
        "POSTCODE": "ABC123",
        "TRANSACTION_DT": datetime.datetime(2023, 8, 27, 12, 0, 0)
    },
    # Add more data here
]

# Insert the data into the 'card_transactions' collection
card_transactions_collection = db['card_transactions']
card_transactions_collection.insert_many(data)

# Create or access the 'moving_average' collection
moving_average_collection = db['moving_average']

# Calculate moving average and standard deviation for each card_id
for card_id in set(doc["CARD_ID"] for doc in data):
    card_transactions = list(card_transactions_collection.find({"CARD_ID": card_id}).sort("TRANSACTION_DT", -1).limit(10))
    transaction_amounts = [doc["AMOUNT"] for doc in card_transactions]

    # Calculate moving average and standard deviation
    num_transactions = len(transaction_amounts)
    moving_avg = sum(transaction_amounts) / num_transactions if num_transactions > 0 else 0
    stddev = statistics.stdev(transaction_amounts) if num_transactions > 1 else 0

    # Insert the calculated data into the 'moving_average' collection
    moving_average_collection.insert_one({
        "CARD_ID": card_id,
        "MOVING_AVG": moving_avg,
        "STANDARD_DEVIATION": stddev
    })

# Print some data from the 'moving_average' collection
for doc in moving_average_collection.find().limit(10):
    print(doc)

# Close the MongoDB connection
client.close()
