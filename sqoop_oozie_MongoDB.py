from pymongo import MongoClient
import statistics

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['capstone_project']

# Assuming you have inserted card_member and member_score data into MongoDB collections

# Calculate moving average and standard deviation
moving_averages = {}
standard_deviations = {}

for card_id in db.card_member.distinct("CARD_ID"):
    transaction_amounts = [doc["AMOUNT"] for doc in db.card_member.find({"CARD_ID": card_id})]

    if len(transaction_amounts) > 0:
        moving_averages[card_id] = sum(transaction_amounts) / len(transaction_amounts)
        standard_deviations[card_id] = statistics.stdev(transaction_amounts)

# Store calculated data in a new MongoDB collection
for card_id in moving_averages:
    db.moving_average_stddev.insert_one({
        "CARD_ID": card_id,
        "MOVING_AVG": moving_averages[card_id],
        "STANDARD_DEVIATION": standard_deviations.get(card_id, 0)
    })

# Close the MongoDB connection
client.close()
