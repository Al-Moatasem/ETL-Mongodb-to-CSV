import json
from unittest import skip
import util
from sys import argv
from datetime import datetime, timedelta

with open('config.json', 'r') as cnfg_file:
    config = json.load(cnfg_file)
    config_mongodb = config['mongodb']

    db_username = config_mongodb['username']
    db_password = config_mongodb['password']
    db_server = config_mongodb['server']
    database = config_mongodb['database']
    collection = config_mongodb['collection']


with open('etl_job_config.json', 'r') as etl_confg_file:
    data = json.load(etl_confg_file)
    last_extraction_date = data['last_scraped_log']['last_scraped']

inputs = argv

# output_file_path = f'Output/Listings/Listing_{output_date_format}.csv'
if __name__ == '__main__':

    ### Validate the input date
    if len(inputs) > 2:
        print('Too many arguments were passed, Please enter a valid date (YYYYMMDD)')
        exit()

    try:
        if len(inputs) == 2:
            # convert input date value into date type
            date = datetime.strptime(inputs[1], '%Y%m%d')
    except:
        print('Please enter a valid date (YYYYMMDD)')
        exit()

    print('Initiating connection to the database')
    client = util.create_mongodb_client(db_username, db_password, db_server)
    colc_airbnb_lst_reviews = util.use_db_collection(
        client, database, collection)

    # cehck if last_extraction_date is not empty/None
    if len(inputs) == 1 and last_extraction_date:
        # the following date of the last extraction
        date = util.get_last_extraction_date() + timedelta(days=1)

    elif len(inputs) == 1:
        date = util.get_first_date_for_last_scraped(
            colc_airbnb_lst_reviews)

    date_str = datetime.strftime(date, '%Y%m%d')

    print(f'Extracting listing reviews last scraped on {date}')
    listing_reviews = util.select_listing_reviews_by_last_scraped_date(
        colc_airbnb_lst_reviews, date, columns='exclude reviews')

    csv_output_path = f'Output/Listings/Listings_{date_str}.csv'
    print(f'Saving data into {csv_output_path}')
    util.export_data_csv(listing_reviews, csv_output_path)

    print(f'Extracting only ids and reviews last scraped on {date}')
    ids_and_reviews = util.select_listing_reviews_by_last_scraped_date(
        colc_airbnb_lst_reviews, date, columns='id and reviews')

    csv_output_path = f'Output/Reviews/Reviews_{date_str}.csv'
    print(f'Saving data into {csv_output_path}')
    util.export_data_csv(ids_and_reviews, csv_output_path)

    print('Logging the last scraped date')
    util.log_etl_last_scraped_date(date_str)


