import json
import pymongo
import pandas as pd
from datetime import datetime, timedelta
from log import log_msg


def create_mongodb_client(db_username, db_password, db_server):
    log_msg('Initiating connection to the database')
    client = pymongo.MongoClient(
        f"mongodb+srv://{db_username}:{db_password}@{db_server}")
    return client


def use_db_collection(client, database, collection):
    database = client[database]
    collection = database[collection]

    return collection


def export_data_csv(data, output_path):
    log_msg(f'Saving data into {output_path}')
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)


def select_listing_reviews_by_last_scraped_date(collection, date, field_list_state=None, fields_list=[]):
    # start_date = datetime.strptime(date, '%Y%m%d')
    log_msg(f'Extracting data last scraped on {date}')
    start_date = date
    end_date = start_date + timedelta(days=1)
    filter = {'last_scraped': {'$gte': start_date, '$lt': end_date}}

    if field_list_state == 'exclude':
        projection = dict.fromkeys(fields_list, False)
    elif field_list_state == 'include':
        projection = fields_list
    else:
        projection = {}

    listing_reviews = collection.find(filter, projection)

    return listing_reviews


def get_first_value_in_field(collection, field_name):

    values = collection.find(
            {}, 
            [field_name]).sort( field_name, 1).distinct(field_name)
    
    return values[0]


def get_last_extraction_date():
    with open('etl_job_config.json', 'r') as etl_confg:
        data = json.load(etl_confg)
        last_extraction_date = data['last_scraped_log']['last_scraped']

        last_extraction_date = datetime.strptime(
            last_extraction_date, '%Y%m%d')

    return last_extraction_date


def log_etl_last_scraped_date(date):
    log_msg('Updating the last scraped date')
    etl_config_file = 'etl_job_config.json'
    # reading the json file
    with open(etl_config_file, 'r') as etl_confg:
        data = json.load(etl_confg)

    data['last_scraped_log']['last_scraped'] = date
    now_str = str(datetime.now())
    data['last_scraped_log']['last_job_execution_date'] = now_str

    # updating the file
    with open(etl_config_file, 'w') as etl_confg:

        json.dump(data, etl_confg)

def validate_date_input(input_date, date_format, message):

    try:
        date = datetime.strptime(input_date, date_format)
        return date
    except Exception as e:
        print(message)
        print(e)