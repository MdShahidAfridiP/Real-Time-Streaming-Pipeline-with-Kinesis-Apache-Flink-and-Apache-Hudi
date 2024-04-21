# Steps 
### Step 1: Create kinesis Streams 
### step 2: upload the jar provided in github repo to S3

#### Download links 
```
https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.13.0/flink-s3-fs-hadoop-1.13.0.jar


https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink-bundle_2.12/0.10.1/hudi-flink-bundle_2.12-0.10.1.jar

```

### step 3: Head over to Kinesis Data Analytics and create a Notebook and upload the jar files while creating notebook 

### step 4 : execute sql commands 

```
%flink.conf
execution.checkpointing.interval 5000

```

```
%flink.ssql(type=update)

DROP TABLE if exists stock_table;

CREATE TABLE stock_table (
    uuid varchar,
    ticker VARCHAR,
    price DOUBLE,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
PARTITIONED BY (ticker)
WITH (
    'connector' = 'kinesis',
    'stream' = 'input-streams',
    'aws.region' = 'us-west-2',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
```

```

%flink.ssql(type=update)

DROP TABLE if exists stock_table_hudi;

CREATE TABLE stock_table_hudi(
    uuid varchar  ,
    ticker VARCHAR,
    price DOUBLE,
    event_time TIMESTAMP(3)
)
PARTITIONED BY (ticker)
WITH (
    'connector' = 'hudi',
    'path' = 's3a://XXXXXXXX/tmp/',
    'table.type' = 'MERGE_ON_READ' ,
    'hoodie.embed.timeline.server' = 'false'
);

```
### step 5 run python code to publish data 
```
try:
    import datetime
    import json
    import random
    import boto3
    import os
    import uuid
    import time
    from faker import Faker

    from dotenv import load_dotenv
    load_dotenv(".env")
except Exception as e:
    pass

global faker
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


while True:
    data = json.dumps(getReferrer())

    global kinesis_client

    kinesis_client = boto3.client('kinesis',
                                  region_name='us-east-1',
                                  aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
                                  aws_secret_access_key=os.getenv("DEV_SECRET_KEY")
                                  )

    res = kinesis_client.put_record(
        StreamName="stock-streams",
        Data=data,
        PartitionKey="1")
    print(data, " " , res)



```

### step 6: Insert into HUDI 
```
%ssql
INSERT INTO stock_table_hudi SELECT * FROM stock_table;

```
