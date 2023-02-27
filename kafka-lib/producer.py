from kafka import KafkaProducer
import pandas as pd
import json
import datetime as dt
import sys

producer = KafkaProducer(
    # Set up servers. Not working today though?
    bootstrap_servers=[
        "ip-172-31-13-101.eu-west-2.compute.internal:9092",
        "ip-172-31-3-80.eu-west-2.compute.internal:9092",
        "ip-172-31-5-217.eu-west-2.compute.internal:9092",
        "ip-172-31-9-237.eu-west-2.compute.internal:9092"
        ],
    security_protocol = "SASL_PLAINTEXT",
    sasl_mechanism = "PLAIN",
    sasl_plain_username = "ec2-user",
    sasl_plain_password = sys.argv[2],
    # We want to encode via json.
    value_serializer = lambda x: json.dumps(x).encode('UTF-8'),
    api_version = (2,0,2)
)

# Watch files or send through get_forecast.py
# while True:
#     look for new files. 
#     new file -> 
#     produce_from_new_file(newfile)

def produce_from_new_file(filepath):
    counter = 0
    topic = "windpowerproject"

    for chunk in pd.read_csv('../ukjan/WEATHER-20230226-221730.csv', chunksize=10): # No hardcoding is required.
        # 10 lines at a time
        key = str(counter).encode()
        chunk_dict = chunk.to_dict()
        message = json.dumps(chunk_dict)
        print(message)
        producer.send(topic=topic, key=key, value=message).get(timeout=30)
        counter +=1 
        print(f"Sent {key} : {message} on topic {topic}")
    producer.close() # Close Producer until next call


produce_from_new_file(sys.argv[1])