import sys
import json
import random
import boto3
import argparse
import datetime as dt
from faker import *

# Create a client with aws service and region
# def create_client(service, region):
# 	return boto3.client(service, region_name=region)

class RecordGenerator(object):
    #generate data used as input for Glue ETL
    
    def __init__(self) :
        self.userid = 0
        self.channelid = 0
        self.genre =""
        self.lastactive = None
        self.title = ""
        self.watchfrequency = 0
        self.etags = None
        
    def get_netflixrecords(self, fake):
        #generates fake metrics
        
        netflixdata = {
            'userid': fake.uuid4(),
            'channelid' : fake.pyint(min_value=1, max_value=50),
            'genre' : random.choice(['thriller', 'comedy', 'romcom', 'fiction']),
            'lastactive': fake.date_time_between(start_date='-10m', end_date='now').isoformat(),
            'title' : fake.name(),
            'watchfrequency' : fake.pyint(min_value=1, max_value=10),
            'etags': fake.uuid4()
        }
        
        data = json.dumps(netflixdata)
        return {'Data': bytes(data, 'utf-8'), 'PartitionKey': 'partition_key'}
    
    def get_netflixrecord(self,rate,fake):
        return [self.get_netflixrecords(fake) for _ in range(rate)]
    
    def dumps_lines(objs):
        for obj in objs:
            yield json.dumps(obj, separators=(',',':')) + '\n'
            
def main():
    
    parser = argparse.ArgumentParser(description='Faker based streaming data generator')

    parser.add_argument('--streamname', action='store', dest='stream_name', help='Provide Kinesis Data Stream name to stream data')
    parser.add_argument('--region', action='store', dest='region', default='us-west-2')

    args = parser.parse_args()
    
    # session = boto3.Session(profile_name= 'bibhusha.ojha')
    
    try:
        fake=Faker()
        kinesis_client = boto3.client('kinesis', args.region)
        rate = 200 #rate at which data is generated
        
        generator = RecordGenerator()
        
        #generate data
        while True:
            fake_data = generator.get_netflixrecord(rate,fake)
            kinesis_client.put_records(StreamName=args.stream_name, Records=fake_data)
            
    except:
        print("Error:", sys.exc_info()[0])
        raise
    
if __name__ == "__main__":
    main()
            
        
        
        