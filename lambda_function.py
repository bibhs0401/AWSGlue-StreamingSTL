import json
import base64
import boto3

def lambda_handler(event, context):
    print(event)
    try:
        dynamo_db = boto3.resource('dynamodb')
        table = dynamo_db.Table('bibhusha-sink-table')
        
        for record in event["Records"]:
            decoded_data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            print(decoded_data)
            
            decoded_data_dic = json.loads(decoded_data)
            
            watch_frequency = decoded_data_dic.get("watchfrequency", 0)
            
            
            if isinstance(watch_frequency, int):
                if watch_frequency ==1:
                    decoded_data_dic["reaction"] = "dislike"
                elif watch_frequency > 10:
                    decoded_data_dic["reaction"] = "favourite"  
                else:
                    decoded_data_dic["reaction"] = "like"  
            else:
                decoded_data_dic["reaction"] = "undetermined"  
            
            with table.batch_writer() as batch_writer:
                batch_writer.put_item(Item=decoded_data_dic)  
    except Exception as e:
        print(str(e))
