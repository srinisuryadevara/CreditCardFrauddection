from pymongo import MongoClient
import datetime

# Connect to the MongoDB server
client = MongoClient('mongodb://localhost:27017/')

# Create or access the 'capstone_project' database
db = client['capstone_project']

# Create or access the 'card_transactions' collection
card_transactions_collection = db['card_transactions']

# Load data from an external source (CSV file, Hive table, etc.)
data = [
    {
        "CARD_ID": "123",
        "MEMBER_ID": "456",
        "AMOUNT": 100.0,
        "POSTCODE": "ABC123",
        "POS_ID": "POS123",
        "TRANSACTION_DT": datetime.datetime(2023, 8, 27, 12, 0, 0),
        "STATUS": "SUCCESS"
    },
    # Add more data here
]

# Insert the data into the 'card_transactions' collection
card_transactions_collection.insert_many(data)

# Print the count of inserted documents
print("Total Number of Transactions Inserted:", card_transactions_collection.count_documents({}))

# Query and print some data from the collection
for document in card_transactions_collection.find().limit(10):
    print(document)

# Close the MongoDB connection
client.close()
