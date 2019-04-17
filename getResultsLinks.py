import requests
import json
import sqlalchemy
import logging
import time
import pytz
import datetime
import pandas as pd
import SQLLoggingHandler
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


def put_in_queue(link):
    logger.info(f"queue_service.put_message({azureQueueImageResults}, {link}")
    queue_service.put_message(azureQueueImageResults, link, time_to_live=-1)
    
    logger.info(f"queue_service.put_message({azureQueueAnalysisRecognizedItems}, {link}")
    queue_service.put_message(azureQueueAnalysisRecognizedItems, link, time_to_live=-1)


def read_next_in_queue():
    try:
        messages = queue_service.get_messages(azureQueueAnalysisResults)

        if messages:
            for message in messages:
                logger.info(f"queue_service.get_messages({azureQueueAnalysisResults}) : {message.content}")
                apikey = "Auth-Token RjtbdmfrfVKFxleFEtzCYGXyAxyxvAFNnyvbGTLkWBdKeHCpHdxQqqlPHhpuLDOCFLxNSxFzVUqgLgScXhsqPFLFTkotmgSUuumXTjSqYxdcQPKjJWGzdFaroMGScLEGNoDIMlGSsQPfphuheNtZrGKp"
                headers = {"Authorization": apikey}
                r = requests.get(message.content, headers = headers)
                dicts = r.json()['results']
                queue_service.delete_message(azureQueueAnalysisResults, message.id, message.pop_receipt)
                for results in dicts:
                    put_in_queue(results["results_link"])
        else:
            logger.info("No Messages for TraxProduct In Queue")
    except Exception as e:
        logger.error("Exception in read_next_in_queue {}" .format(e))


if __name__ == '__main__':
    loggerName = "CharlesTraxRecogQueue"
    logger = logging.getLogger(loggerName)
    init_logger()
    try:
        azureQueueAccountName = "traxwebjobs"
        azureQueueKey = ""
        azureQueueAnalysisRecognizedItems = "recognizeditems-processing"
        azureQueueAnalysisResults = "analysis-results"
        azureQueueImageResults = "image-processing"

        queue_service = QueueService(account_name=azureQueueAccountName, account_key=azureQueueKey)

        while True:
            #create queue if doesnt exist
            if not queue_service.exists(azureQueueAnalysisRecognizedItems):
                queue_service.create_queue(azureQueueAnalysisRecognizedItems)

            if not queue_service.exists(azureQueueImageResults):
                queue_service.create_queue(azureQueueImageResults)
            
            #get queue count
            metadata = queue_service.get_queue_metadata(azureQueueAnalysisResults)
            queue_count = metadata.approximate_message_count

            if queue_count > 0:
                read_next_in_queue()
            else:
                logger.info("time.sleep(1)")
                time.sleep(1)

    except Exception as e:
        logger.error("Exception in main {}" .format(e))
