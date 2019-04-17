import pandas as pd
import json
import flask, flask_restplus, flask_jwt
import flask_jwt
import requests
import sqlalchemy
import datetime
import pytz
import time
import pyodbc
from datetime import timedelta
from sqlalchemy import create_engine
from flask_jwt import JWT
from flask import Flask, Blueprint, Response
from flask_restplus import Resource, Api
from pandas.io.json import json_normalize
from multiprocessing import Pool


app = Flask(__name__)
trax = Blueprint('api', __name__, url_prefix='/TraxData')
api = Api(trax, version='1.0', title='Trax Data Ingestion API', description = 'Trax API DATA Whatever',

default = 'Trax API DATA', default_label = None)
app.register_blueprint(trax)


##################################################################################################################################################################
@api.route('/RecognizedItems')
class RecognizedItems(Resource):

    @classmethod
    def get(self):
        try:
            dateFrom = "2019-03-26 00:00:00"
            dateTo = "2019-03-26 23:59:59"

            results_links = page_parse(dateFrom, dateTo)
            agents = 16

            with Pool(agents) as p:
                records = p.map(final_frame, results_links)

            #logger.info("total runtime: {} seconds".format(end))
            
        except Exception as e:
            print(e)

##################################################################################################################################################################

@api.route('/Images')
class Images(Resource):

    @classmethod
    def get(self):
        try:
            dateFrom = "2019-03-26 00:00:00"
            dateTo = "2019-03-26 12:59:59"

            results_links = page_parse(dateFrom, dateTo)
            agents = 16

            with Pool(agents) as p:
                records = p.map(image_frame, results_links)
            
            #runtime = time.time() - start_time

            #logger.info("total runtime: {} seconds".format(end))
            
        except Exception as e:
            print(e)

##################################################################################################################################################################



@api.route('/Products')
class Products(Resource):

    @classmethod
    def get(self):
        try:
            results_links = product_parse()
            agents = 16

            with Pool(agents) as p:
                records = p.map(get_products, results_links)
            
        except Exception as e:
            print(e)


##################################################################################################################################################################

def change_datetime_to_epoch(somedatetime):
    temp_date = datetime.datetime.strptime(somedatetime, "%Y-%m-%d %H:%M:%S")
    return (temp_date + timedelta(hours=-6)).timestamp()

def product_parse():
    try:
        base_url = "https://services.traxretail.com/api/V1/ccbottlersus/entity/product?sort=product_name&page=0&amp;per_page=200"
        base = "https://services.traxretail.com"
        apikey = ""
        headers = {"Authorization": apikey}
        r = requests.get(base_url, headers=headers)
        page_count = r.json()['metadata']['page_count']
        # total_count = r.json()['metadata']['total_count']
    

        page_list = []
        print(int(page_count))
        i = range(0, int(page_count))
        for num in i:
            link = base + '/api/V4/ccbottlersus/entity/products?sort=product_name&page={}&per_page=200'.format(num)
            page_list.append(link)

        return page_list
    except Exception as e:
        print(e)

def page_parse(dateFrom, dateTo):
    try:
        if not dateFrom:
            raise ValueError("page_parse dateFrom needs to be passed")

        if not dateTo:
            raise ValueError("page_parse dateTo needs to be passed")


        results_links = []
        page_list = []

        #TODO .999999 is to account for milliseconds after 59 second
        dateFrom = change_datetime_to_epoch(dateFrom)

        #TODO .999999 is to account for milliseconds after 59 second
        dateTo = change_datetime_to_epoch(dateTo)


        base_url = "https://services.traxretail.com/api/V4/ccbottlersus/analysis-results?from={}&to={}&page=0&per_page=200".format(dateFrom, dateTo)
        base = "https://services.traxretail.com"
        apikey = ""
        headers = {"Authorization": apikey}
        r = requests.get(base_url, headers=headers)

        page_count = r.json()['metadata']['page_count']
        lastpage = page_count

        i = range(0, int(lastpage))
        for num in i:
            link = base + '/api/V4/ccbottlersus/analysis-results?from={}&to={}&page={}&per_page=200'.format(dateFrom, dateTo, num)
            page_list.append(link)
        for page in page_list:
            apikey = ""
            headers = {"Authorization": apikey}
            r = requests.get(page, headers = headers)
            dicts = r.json()['results']
            for results in dicts:
                results_links.append(results["results_link"])

        return results_links
    except Exception as e:
        print(e)
        
def final_frame(link):
    try:
        fmt = '%Y-%m-%d %H:%M:%S'
        slink = (requests.get(link).json())

        item_count = len(slink['details']['recognized_items'])
        current_session_uid = slink['session_uid']
        sinfo = pd.DataFrame(json_normalize(data = slink['details'], record_path=['recognized_items'], errors='ignore'))

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

            engine = create_engine('mssql+pyodbc://user:pass@ccbcusqldev.database.windows.net/trax-api-data-dev?driver=ODBC+Driver+13+for+SQL+Server')

            final_frame.to_sql("LoadedRecognizedItems", engine, if_exists='append', chunksize=None, index=False)
        else:
            print("No Recognized Items in session_uid: {}, {}".format(current_session_uid, link))
    except ValueError:
        print("Failed to decode JSON in {}".format(link))
    except KeyError:
        print("No Recognized Items {}".format(link))
    except Exception:
        print("Exception in final_frame")
        print("{}".format(link))


def image_frame(link): #requests responses
    try:
        fmt = '%Y-%m-%d %H:%M:%S'
        scene_df = pd.DataFrame()
        quality_df = pd.DataFrame()
        slink = (requests.get(link)).json() #response into readable data

        #check for image data in json
        image_count = len(slink['details']['images'])
        current_session_uid = slink['session_uid']
        #logger.info(f"session_uid: {current_session_uid} , Image Count: {image_count}")

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

            engine = create_engine('mssql+pyodbc://user:pass@ccbcusqldev.database.windows.net/trax-api-data-dev?driver=ODBC+Driver+13+for+SQL+Server')

            image_frame.to_sql("LoadedImages", engine, if_exists='append',chunksize=None, index=False)
        else:
            print("No Images in session_uid: {} , link: {}".format(current_session_uid, link))

    except ValueError:
        print("Failed to decode JSON in {}".format(link))
    except KeyError:
        print("No Images in JSON {}".format(link))
    except Exception:
        print("Exception in final_frame")
        print("{}".format(link))

##################################################################################################################################################################

def get_products(page):
    #init_logger()
    attributes = pd.DataFrame()
    df = pd.DataFrame()

    apikey = ""
    headers = {"Authorization": apikey}

    #with open(path + '\\Products_RAW{}.json'.format(today), 'a') as rson:
    try:
        #logger.info(f"HTTP GET: {message}")
        r = requests.get(page, headers = headers).json()
        df = pd.concat([df, pd.DataFrame(r['product'])], sort=True, ignore_index=True) 
        for item in r['product']:
            attr = pd.DataFrame([pd.Series(item['product_additional_attributes'])])
            #json.dump(item, rson)
            attributes = pd.concat([attributes, attr], axis=0, sort=True,ignore_index = True)
    #product_to_lake(rson.name)


        #df.drop(['product_additional_attributes'], axis = 1, inplace=True)
        #df.drop(['alternative_designs'], axis = 1, inplace=True)
        #df.drop(['images'], axis = 1, inplace=True)
        df = pd.merge(attributes, df, how='outer', left_index=True, right_index=True)
#######################################################################################################
        df.rename(columns={"item_id": "ItemID"}, inplace=True)
        df.rename(columns={"product_name": "ProductName"}, inplace=True)
        df.rename(columns={"product_local_name": "ProductLocalName"}, inplace=True)
        df.rename(columns={"product_short_name": "ProductShortName"}, inplace=True)
        df.rename(columns={"product_uuid": "ProductID"}, inplace=True)
        df.rename(columns={"product_type": "ProductType"}, inplace=True)
        df.rename(columns={"size": "Size"}, inplace=True)
        df.rename(columns={"subcategory_local_name": "SubcategoryLocalName"}, inplace=True)
        df.rename(columns={"product_client_code": "ProductClientCode"}, inplace=True)
        df.rename(columns={"number_of_subpackages": "NumberOfSubpackages"}, inplace=True)
        df.rename(columns={"manufacturer_name": "ManufacturerName"}, inplace=True)
        df.rename(columns={"manufacturer_local_name": "ManufacturerLocalName"}, inplace=True)
        df.rename(columns={"is_deleted": "Deleted"}, inplace=True)
        df.rename(columns={"is_active": "Active"}, inplace=True)
        df.rename(columns={"discovered_by_brand_watch": "DiscoveredByBrandWatch"}, inplace=True)
        df.rename(columns={"container_type": "ContainerType"}, inplace=True)
        df.rename(columns={"category_name": "CategoryName"}, inplace=True)
        df.rename(columns={"category_local_name": "CategoryLocalName"}, inplace=True)
        df.rename(columns={"brand_name": "BrandName"}, inplace=True)
        df.rename(columns={"brand_local_name": "BrandLocalName"}, inplace=True)
        df.rename(columns={"Nielsen_UPC": "NielsenUPC"}, inplace=True)
#######################################################################################################
        if "product_item_code" not in df:
            df['ProductItemCode'] = pd.Series()
        else:
            df.rename(columns={"product_item_code": "ProductItemCode"}, inplace=True)
#######################################################################################################
        if "att3" not in df:
            df['PackageGroup'] = pd.Series()
        else:
            df.rename(columns={"att3": "PackageGroup"}, inplace=True)
#######################################################################################################       
        if "att2" not in df:
            df['Trademark'] = pd.Series()
        else:
            df.rename(columns={"att2": "Trademark"}, inplace=True)
#######################################################################################################        
        if "att1" not in df:
            df['PackageType'] = pd.Series()
        else:
            df.rename(columns={"att1": "PackageType"}, inplace=True)
#######################################################################################################
        if "att4" not in df:
            df['SSD_Still'] = pd.Series()
        else:
            df.rename(columns={"att4": "SSD_Still"}, inplace=True)
#######################################################################################################
        if "UPC Matched" not in df:
            df['UPCMatched'] = pd.Series()
        else:
            df.rename(columns={"UPC Matched": "UPCMatched"}, inplace=True)
#######################################################################################################        
        if "Container Material" not in df:
            df['ContainerMaterial'] = pd.Series()
        else:
            df.rename(columns={"Container Material": "ContainerMaterial"}, inplace=True)
#######################################################################################################
        if "Innovation Brand" not in df:
            df['InnovationBrand'] = pd.Series()
        else:
            df.rename(columns={"Innovation Brand": "InnovationBrand"}, inplace=True)
#######################################################################################################
        if "Premium SSD" not in df:
            df['PremiumSSD'] = pd.Series()
        else:
            df.rename(columns={"Premium SSD": "PremiumSSD"}, inplace=True)
#######################################################################################################
        if "Transaction Packages" not in df:
            df['TransactionPackages'] = pd.Series()
        else:
            df.rename(columns={"Transaction Packages": "TransactionPackages"}, inplace=True)
#######################################################################################################
        if "Issues" not in df:
            df['Issues'] = pd.Series()
        else:
            df.rename(columns={"Issues": "Issues"}, inplace=True)
#######################################################################################################
        if "Resolution" not in df:
            df['Resolution'] = pd.Series()
        else:
            df.rename(columns={"Resolution": "Resolution"}, inplace=True)
#######################################################################################################
        if "units" not in df:
            df['Units'] = pd.Series()
        else:
            df.rename(columns={"units": "Units"}, inplace=True)
#######################################################################################################
        if "unit measurement" not in df:
            df['UnitOfMeasure'] = pd.Series()
        else:
            df.rename(columns={"unit": "UnitOfMeasure"}, inplace=True)
#######################################################################################################
        df['UpdateDateTime'] = datetime.datetime.now()
        df['JobName'] = 'TraxProduct'
#######################################################################################################
        df=df[['ItemID', 'ProductName', 'ProductLocalName', 'ProductShortName', 'ProductID', 'ProductType', 'Size', 'SubcategoryLocalName', 'UnitOfMeasure', 'Units', 'ProductItemCode'
        ,'ProductClientCode', 'NumberOfSubpackages', 'ManufacturerName', 'ManufacturerLocalName', 'Deleted', 'Active', 'DiscoveredByBrandWatch', 'ContainerType', 'CategoryName'
        ,'CategoryLocalName', 'BrandName', 'BrandLocalName', 'PackageGroup', 'Trademark', 'PackageType', 'SSD_Still', 'NielsenUPC', 'UPCMatched', 'ContainerMaterial', 'InnovationBrand'
        ,'PremiumSSD', 'TransactionPackages', 'Issues', 'Resolution', 'UpdateDateTime', 'JobName']]

        df.fillna('', inplace=True)
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # ~~~~~~~~~~~~~~~~~~~~~~FINAL DETAILS, SENDING JOB LOG~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # log stream sent off to the data lake
        #product_to_lake(pdl.name)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~TO SQL~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        engine = create_engine('mssql+pyodbc://user:pass@ccbcusqldev.database.windows.net/trax-api-data-dev?driver=ODBC+Driver+13+for+SQL+Server')


        df.to_sql("LoadedProduct", engine, if_exists='append', chunksize= None, index=False)

    except Exception as e:
        print("Exception in get_product: {}".format(e))


##################################################################################################################################################################
##################################################################################################################################################################
##################################################################################################################################################################
api.add_resource(RecognizedItems, '/RecognizedItems')
api.add_resource(Images, '/Images')
api.add_resource(Products, '/Products')

if __name__ == '__main__':
    app.run(port=5000, debug=True)
