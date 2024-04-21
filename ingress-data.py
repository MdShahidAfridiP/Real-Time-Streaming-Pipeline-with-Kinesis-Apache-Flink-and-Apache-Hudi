
import datetime
import json
import random
import boto3
import os
import uuid
import time
from faker import Faker

faker = Faker()

def getReferrer():
    data = {}
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['uuid'] = str(uuid.uuid4())
    data['event_time'] = str_now

    data['ticker'] = random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV'])
    price = random.random() * 100
    data['price'] = round(price, 2)
    return data

kinesis_client = boto3.client('kinesis',
                                  region_name='us-east-1',
                                  aws_access_key_id="AKIAU6GD3ETSMSDU2KG4",
                                  aws_secret_access_key="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"
                                  )
while True:
    data = json.dumps(getReferrer())

    res = kinesis_client.put_record(
        StreamName="input-stream",
        Data=data,
        PartitionKey="1")
    print(data, " " , res)
