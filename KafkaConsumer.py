from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.serializers import PickleSerializer
import datetime

# Setup GROUP_ID in such a way that it's unique every time
GROUP_ID = "amitgoelkafkaspark" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if __name__ == "__main__":
    conf = SparkConf().setAppName("CreditCardFraudDetection").setMaster("local[*]")
    ssc = StreamingContext(conf, 1)
    ssc.sparkContext.setSerializer(PickleSerializer())

    # Set parameters for Kafka stream
    kafka_params = {
        "bootstrap.servers": "100.24.223.181:9092",
        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "true"
    }

    # Subscribe to Kafka topic
    topics = ["transactions-topic-verified"]

    # Create Kafka stream
    stream = KafkaUtils.createDirectStream(ssc, topics, kafka_params)

    # Use map transformation on input stream and get the value part
    jds = stream.map(lambda x: x[1])

    # Print count of records present
    jds.foreachRDD(lambda x: print("Total Number of Transactions to be Processed : " + str(x.count()) + "\n"))

    # Print all the data
    jds.foreachRDD(lambda rdd: rdd.foreach(lambda a: print(a)))

    # Map data from input stream to CreditCardFraudDetection class constructor
    jds_mapped = jds.map(lambda x: CreditCardFraudDetection(x))

    # Call FraudDetection method in CreditCardFraudDetection class
    jds_mapped.foreachRDD(lambda rdd: rdd.foreach(lambda x: x.FraudDetection(x)))

    # Print current time stamp before starting
    print("\nStart Time : " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")

    # Start Spark Streaming
    ssc.start()

    # Await Termination to respond to Ctrl+C and gracefully close Spark Streaming
    ssc.awaitTermination()

    # Print current time stamp before closing
    print("\nEnd Time : " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")

    # Close Spark Streaming
    ssc.stop()
