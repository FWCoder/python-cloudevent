import io
import os
import gc
import sys
from io import BytesIO
import boto3
import numpy as np
from cloudevents.http import from_http
from flask import Flask, request
from flask_cors import CORS
from kafka import KafkaProducer
import json, uuid, datetime
import logging

kogitoKafkaBootstrapURL="kogito-kafka-kafka-bootstrap.xray-kogito-process.svc:9092"
xrayTopic='xray'
test_data={"Records":[{"eventVersion":"2.2","eventSource":"ceph:s3","awsRegion":"","eventTime":"2021-02-19 02:03:08.330496Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"xraylab-0001"},"requestParameters":{"sourceIPAddress":""},"responseElements":{"x-amz-request-id":"f089746f-485e-410a-990a-e00f621fcdc3.24146.351","x-amz-id-2":"5e52-s3a-s3a"},"s3":{"s3SchemaVersion":"1.0","configurationId":"xray-images","bucket":{"name":"xraylab-0001","ownerIdentity":{"principalId":"xraylab-0001"},"arn":"arn:aws:s3:::xraylab-0001","id":"f089746f-485e-410a-990a-e00f621fcdc3.24152.1"},"object":{"key":"demo_Micheal Phillips_6580_1947-05-22_2017-04-27.jpeg","size":108622,"etag":"819c1e1c1b4c7b9853301ba2e6c300f1","versionId":"","sequencer":"5C1C2F6049974614","metadata":[{"key":"x-amz-content-sha256","val":"f7d632b7b02aeb97248e20e32c02dc0d2a8336927cbe487dc4d218dd1864e775"},{"key":"x-amz-date","val":"20210219T020308Z"}],"tags":[]}},"eventId":"1613700188.340170.819c1e1c1b4c7b9853301ba2e6c300f1","opaqueData":""}]}

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

app = Flask(__name__)
CORS(app)

@app.route("/", methods=["POST"])
def home():
    # Retrieve the CloudEvent
    event = from_http(request.headers, request.get_data())
    
    # Process the event
    process_event(event.data)

    return "", 204

def process_event(data):
    """Main function to process data received by the container image."""

    logging.info(data)

    try:
        producer = KafkaProducer(bootstrap_servers=[kogitoKafkaBootstrapURL])
        extracted_data = extract_cloud_event_data(test_data)
        event = {}
        event['id'] = str(uuid.uuid4())
        event['source'] = ''
        event['type'] = 'XrayEvent'
        event['specversion'] = '0.3'
        event['time'] = datetime.datetime.utcnow().isoformat('T') + 'Z'
        event['data'] = extracted_data
        event_json_str = json.dumps(event)
        producer.send(xrayTopic, bytes(event_json_str, "UTF-8")).get(timeout=10)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

def extract_cloud_event_data(data):
    record=data['Records'][0]
    event_name=record['eventName']
    name=record['s3']['bucket']['name']
    key=record['s3']['object']['key']
    data_out = {'eventName':event_name, 'name':name, 'key':key}
    return data_out

# Launch Flask server
if __name__ == '__main__':
    app.run(host='0.0.0.0')