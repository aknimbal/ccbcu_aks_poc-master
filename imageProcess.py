import requests
import json
import sqlalchemy
import logging
import time
import pytz
import datetime
import pandas as pd
import SQLLoggingHandler
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
from azure.storage.queue import QueueService

def init_logger():
    logger.setLevel(logging.INFO)

    # sqlite handler
    sh = SQLLoggingHandler.SQLHandler(host="", port=1433, user="", passwd="", database="")
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

    # stdout handler
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    logger.addHandler(console)



def read_next_in_queue():
    try:
        messages = queue_service.get_messages(azureQueueImageProcess, num_messages=1)
        if messages:
            for message in messages:
                message_id = message.id
                image_frame(message.content)
                queue_service.delete_message(azureQueueImageProcess, message_id, message.pop_receipt)
        else:
            init_logger()
            logger.info("No Messages for TraxRecognizedItems In Queue")
    except Exception as e:
        init_logger()
        logger.error("Exception in read_next_in_queue {}" .format(e))


def image_frame(link): #requests responses
    try:
        logger.info(f"image_frame({link})")
        fmt = '%Y-%m-%d %H:%M:%S'
        scene_df = pd.DataFrame()
        quality_df = pd.DataFrame()
        slink = (requests.get(link)).json() #response into readable data

        #check for image data in json
        image_count = len(slink['details']['images'])
        current_session_uid = slink['session_uid']
        logger.info(f"session_uid: {current_session_uid} , Image Count: {image_count}")

        if image_count > 0:
            scene_data = pd.DataFrame(json_normalize(data = slink['details'], record_path = ['images', 'scene_images'], 
            meta=[['images', 'scene_uid'],['images', 'task_code'],['images', 'task_name']], errors='ignore'))
            quality_issues = pd.DataFrame(json_normalize(data = slink['details'], record_path = ['images', 'scene_images', 'quality_issues'], meta = [['images', 'scene_images', 'image_uid']],  errors='ignore'))
            quality_df = pd.concat([quality_df, quality_issues], axis = 0, sort=True)

            scene_data['session_uid'] = slink['session_uid'] #adds top level info to dataframe
            scene_data['session_date'] = slink['session_date'] #adds top level info to dataframe
            scene_data['session_start_time'] = slink['session_start_time'] #adds top level info to dataframe
            scene_data['visitor_identifier'] = slink['visitor_identifier'] #adds top level info to dataframe
            scene_data['store_number'] = slink['store_number']
            scene_data['session_date'] = slink['session_date']


            scene_df = pd.concat([scene_df, scene_data], axis = 0, sort = True)		
            quality_df.rename(columns={"images.scene_images.image_uid": "image_uid"}, inplace=True)


            scene_df.rename(columns={"images.scene_uid": "scene_uid"}, inplace=True)
            scene_df.rename(columns={"images.task_code": "task_code"}, inplace=True)
            scene_df.rename(columns={"images.task_name": "task_name"}, inplace=True)

            image_urls = pd.DataFrame(scene_df['image_urls'].apply(pd.Series))

            scene_df.drop(['image_urls'], axis = 1, inplace=True)
            scene_df.drop(['quality_issues'], axis = 1, inplace=True)
            scene_df['ImageURL'] = image_urls['original'] #adds image from split

            image_frame = pd.merge(scene_df, quality_df, how='left', sort=False)

            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~DataFrame Remodeling~~~~~~~~~~~~~~~~~~~~~~~~~~
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

            # Renaming Columns to how the Business wants them
            image_frame.rename(columns={"scene_uid": "SceneID"}, inplace=True)
            image_frame.rename(columns={"task_code": "TaskCode"}, inplace=True)
            image_frame.rename(columns={"task_name": "TaskName"}, inplace=True)
            image_frame.rename(columns={"code": "QualityIssueCode"}, inplace=True)
            image_frame.rename(columns={"value": "QualityIssueValue"}, inplace=True)
            image_frame.rename(columns={"image_uid": "ImageID"}, inplace=True)
            image_frame.rename(columns={"session_uid": "SessionID"}, inplace=True)
            image_frame.rename(columns={"session_start_time": "SessionDateTime"}, inplace=True)
            image_frame.rename(columns={"session_date": "SessionDate"}, inplace=True)
            image_frame.rename(columns={"visitor_identifier": "EmailAddress"}, inplace=True)
            image_frame.rename(columns={"store_number": "StoreNumber"}, inplace=True)

            image_frame = image_frame[image_frame.StoreNumber.str.contains("50&")]
            image_frame['StoreNumber'] = image_frame['StoreNumber'].str[3:]

            image_frame['ImageCaptureTime'] = pd.to_datetime(image_frame['capture_time'], unit='s') #convert capture_time to UTC and change column name
            image_frame['SessionDateTime'] = pd.to_datetime(image_frame['SessionDateTime'], unit='s')
            image_frame.drop(['capture_time'], axis = 1, inplace=True) # drops the original capture time from dataframe
            image_frame['JobName'] = 'Analysis_Results_Images' # New column that contains the Job Name
            image_frame['UpdateDateTime'] = datetime.datetime.now(tz=pytz.utc).strftime(fmt) # New Column that contains the time the Job Ran

            #Reorganizing the columns of the DataFrame
            image_frame = image_frame[['SessionID', 'ImageID', 'QualityIssueCode', 'StoreNumber', 'SessionDate', 'SessionDateTime', 'EmailAddress', 'TaskCode', 'TaskName', 'ImageURL',
            'ImageCaptureTime', 'QualityIssueValue', 'UpdateDateTime', 'JobName']]

            #Get rid of the NULL values in the dataframe
            image_frame.fillna('', inplace=True)

            engine = create_engine('mssql+pyodbc://username:password@yourdatabase.windows.net/table?driver=ODBC+Driver+13+for+SQL+Server') #whatever ODBC driver you're using

            image_frame.to_sql("TraxImages", engine, if_exists='append',chunksize=None, index=False)
        else:
            logger.warn(f"No Images in session_uid: {current_session_uid} , link: {link}")

    except ValueError:
        logger.error("Failed to decode JSON in {}".format(link))
    except KeyError:
        logger.error("No Images in  {}".format(link))
    except Exception:
        logger.error("Exception in final_frame")
        logger.warn(link)


if __name__ == '__main__':
    loggerName = "ResultsLinksProcess"
    logger = logging.getLogger(loggerName)

    azureQueueAccountName = "traxwebjobs"
    azureQueueKey = ""
    azureQueueImageProcess = "image-processing"
    queue_service = QueueService(account_name=azureQueueAccountName, account_key=azureQueueKey)
    try:

        while True:
            #get queue count
            metadata = queue_service.get_queue_metadata(azureQueueImageProcess)
            queue_count = metadata.approximate_message_count

            if queue_count > 0:
                read_next_in_queue()
            else:
                logger.info("time.sleep(1)")
                time.sleep(1)



    except Exception as e:
        init_logger()
        logger.error("Exception in main {}" .format(e))
