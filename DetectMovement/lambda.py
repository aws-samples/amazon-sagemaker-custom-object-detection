from threading import Thread, Event, Timer
import os  
import json  
import numpy as np  
import awscam  
import cv2  
import time  
import greengrasssdk 
import datetime
import boto3
import datetime



client = greengrasssdk.client('iot-data')
iotTopic = '$aws/things/{}/infer'.format(os.environ['AWS_IOT_THING_NAME'])
thingName = os.environ['AWS_IOT_THING_NAME'];

motionThreshold = 200
bucketName = 'bucket-name-here'

def load_config_values():
    try:
        ssm = boto3.client('ssm', region_name='us-east-1')
        response = ssm.get_parameters(
            Names=[
                '/Cameras/{cameraKey}/CameraBucket'.format(cameraKey=thingName),
                '/Cameras/{cameraKey}/MotionThreshold'.format(cameraKey=thingName)
            ],
            WithDecryption=False
        )
        global bucketName
        global motionThreshold
        bucketName = response['Parameters'][0]['Value']
        motionThreshold = int(response['Parameters'][1]['Value'])
    except Exception as e:
        log("Getting SSM parameters failed: " + str(e))

def function_handler(event, context):
    return

def log(message):
    client.publish(topic=iotTopic, payload=message)

def mse(imageA, imageB):
	err = np.sum((imageA.astype("float") - imageB.astype("float")) ** 2)
	err /= float(imageA.shape[0] * imageA.shape[1])
	return err

def push_to_s3(frame, movement):
    try:
        timestamp = 2000000000 - int(time.time())
        key = "frames/{}/{}.jpg".format(thingName, timestamp)
        s3 = boto3.client('s3')
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
        
        frameResize = cv2.resize(frame, (1344, 760))
        
        _, jpg_data = cv2.imencode('.jpg', frameResize, encode_param)
        response = s3.put_object(Body=jpg_data.tostring(),
                                 Bucket=bucketName,
                                 ContentType='image/jpeg',
                                 Key=key,    
                                 Metadata={
                                    'movement': str(movement)
                                 })
    except Exception as e:
        log("Pushing to S3 failed: " + str(e))

def loop():
    try:  
        nextFrameIndexTime = datetime.datetime.now() + datetime.timedelta(minutes = 1)
        log("Inside main loop.")
        ret, previousFrame = awscam.getLastFrame()
        doLoop = True
        while doLoop:
            ret, currentFrame = awscam.getLastFrame()
            err = mse(previousFrame, currentFrame)
            if (err > motionThreshold) or (datetime.datetime.now() >= nextFrameIndexTime):
                push_to_s3(currentFrame, err)
                nextFrameIndexTime = datetime.datetime.now() + datetime.timedelta(minutes = 1)
            previousFrame = currentFrame
    except Exception as e:
        log("Error: " + str(e))

    Timer(15, loop).start()

log("loading config values..")
load_config_values()

log("Starting program...")
loop()
