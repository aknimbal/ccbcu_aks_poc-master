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
        messages = queue_service.get_messages(azureQueueRecognizedItems, num_messages=1)
        if messages:
            for message in messages:
                message_id = message.id
                final_frame(message.content)
                queue_service.delete_message(azureQueueRecognizedItems, message_id, message.pop_receipt)
        else:
            init_logger()
            logger.info("No Messages for TraxRecognizedItems In Queue")
    except Exception as e:
        init_logger()
        logger.error("Exception in read_next_in_queue {}" .format(e))


def final_frame(link):
    try:
        logger.info(f"items_frame({link})")
        fmt = '%Y-%m-%d %H:%M:%S'
        slink = (requests.get(link).json())

        item_count = len(slink['details']['recognized_items'])
        current_session_uid = slink['session_uid']
        sinfo = pd.DataFrame(json_normalize(data = slink['details'], record_path=['recognized_items'], errors='ignore'))
        logger.info(f"session_uid: {current_session_uid} , Recognized Item Count: {item_count}")

        if item_count > 0:
            sinfo['SessionID'] = slink['session_uid']
            sinfo['SessionDate'] = slink['session_date']
            sinfo['SessionDateTime'] = slink['session_start_time']
            sinfo['EmailAddress'] = slink['visitor_identifier']
            sinfo['StoreNumber'] = slink['store_number']
            recog_items = pd.DataFrame(json_normalize(data = slink['details'], record_path = ['recognized_items', 'items'], meta=[['recognized_items', 'scene_uid']], errors='ignore'))
            

            recog_items.rename(columns={"recognized_items.scene_uid": "scene_uid"}, inplace=True)
            sinfo.drop(['items'], axis = 1, inplace = True)

            final_frame = pd.merge(recog_items, sinfo, how = 'left', sort=False)
            item_counts = pd.DataFrame(final_frame['count'].apply(pd.Series))

            final_frame = pd.concat([final_frame,item_counts], axis=1, sort=False)

            if "product_uuid" not in final_frame:
                final_frame['ProductID'] = pd.Series()
            else:
                final_frame.rename(columns={"product_uuid": "ProductID"}, inplace=True)

            final_frame.rename(columns={"scene_uid": "SceneID"}, inplace=True)
            final_frame.rename(columns={"task_code": "TaskCode"}, inplace=True)
            final_frame.rename(columns={"task_name": "TaskName"}, inplace=True)
            final_frame.rename(columns={"code": "ProductCode"}, inplace=True)
            final_frame.rename(columns={"name": "ProductName"}, inplace=True)
            final_frame.rename(columns={"type": "ProductType"}, inplace=True)
            final_frame.rename(columns={"total": "CountTotal"}, inplace=True)
            final_frame.rename(columns={"front": "CountFront"}, inplace=True)
            final_frame.rename(columns={"scene_uid": "SceneID"}, inplace=True)

            final_frame = final_frame[final_frame.StoreNumber.str.contains("50&")]
            final_frame['StoreNumber'] = final_frame['StoreNumber'].str[-9:]
            final_frame['UpdateDateTime'] = datetime.datetime.now(tz=pytz.utc).strftime(fmt)
            final_frame['JobName'] = 'Recognized Items'
            final_frame['SessionDateTime'] = pd.to_datetime(final_frame['SessionDateTime'], unit='s')
            final_frame.fillna('', inplace=True)

            final_frame = final_frame[['SessionID', 'SceneID', 'ProductID', 'StoreNumber', 'SessionDate', 'SessionDateTime', 'EmailAddress', 'TaskCode', 'TaskName', 'id',
            'ProductCode', 'ProductName', 'ProductType', 'CountTotal', 'CountFront', 'UpdateDateTime', 'JobName']]

            engine = create_engine('mssql+pyodbc://username:password@yourdatabase.database.windows.net/databasename?driver=ODBC+Driver+13+for+SQL+Server')

            final_frame.to_sql("LoadedRecognizedItems", engine, if_exists='append', chunksize=None, index=False)
        else:
            logger.warn(f"No Recognized Items in session_uid: {current_session_uid} , link: {link}")
    except ValueError:
        logger.error("Failed to decode JSON in {}".format(link))
    except KeyError:
        logger.error("No Recognized Items {}".format(link))
    except Exception:
        logger.error("Exception in final_frame")
        logger.warn(link)
        


if __name__ == '__main__':
    loggerName = "ResultsLinksProcess"
    logger = logging.getLogger(loggerName)

    azureQueueAccountName = "traxwebjobs"
    azureQueueKey = ""
    azureQueueRecognizedItems = "recognizeditems-processing"
    queue_service = QueueService(account_name=azureQueueAccountName, account_key=azureQueueKey)
    try:
        read_next_in_queue()

    except Exception as e:
        init_logger()
        logger.error("Exception in main {}" .format(e))
