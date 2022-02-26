import json
import util
from sys import argv
from datetime import datetime, timedelta

def main(input_date = None):
    
    print('Validating ...')
    
    if len(input_date) > 2:
        print('Too many arguments were passed, Please enter a valid date (YYYYMMDD)')
        return None

    if len(input_date) == 2:
        date = util.validate_date_input(input_date[1], '%Y%m%d', 'Please enter a valid date (YYYYMMDD)')

    with open('etl_job_config.json', 'r') as etl_confg_file:
        data = json.load(etl_confg_file)
        last_extraction_date = data['last_scraped_log']['last_scraped']

    with open('config.json', 'r') as cnfg_file:
        config = json.load(cnfg_file)
        config_mongodb = config['mongodb']

        db_username = config_mongodb['username']
        db_password = config_mongodb['password']
        db_server = config_mongodb['server']
        database = config_mongodb['database']
        collection = config_mongodb['collection']

    if len(input_date) == 1 and last_extraction_date:
        # the following date of the last extraction
        date = util.get_last_extraction_date() + timedelta(days=1)

    elif len(input_date) == 1:
        date = util.get_first_value_in_field(
                    util.use_db_collection( 
                        util.create_mongodb_client(db_username, db_password, db_server), 
                        database, 
                        collection
                    ), 
                    'last_scraped'
                )
    date_str = datetime.strftime(date, '%Y%m%d')

    print('Initiating connection to the database')
    client = util.create_mongodb_client(db_username, db_password, db_server)
    colc_airbnb_lst_reviews = util.use_db_collection(
        client, database, collection)

    print(f'Extracting listing reviews last scraped on {date}')
    listing_reviews = util.select_listing_reviews_by_last_scraped_date(
                            colc_airbnb_lst_reviews, 
                            date, 
                            field_list_state='exclude', 
                            fields_list=['reviews']
                        )

    csv_output_path = f'Output/Listings/Listings_{date_str}.csv'
    print(f'Saving data into {csv_output_path}')
    util.export_data_csv(listing_reviews, csv_output_path)

    print(f'Extracting only ids and reviews last scraped on {date}')
    ids_and_reviews = util.select_listing_reviews_by_last_scraped_date(
                            colc_airbnb_lst_reviews, 
                            date, 
                            field_list_state='include', 
                            fields_list=['_id','reviews']
                        )

    csv_output_path = f'Output/Reviews/Reviews_{date_str}.csv'
    print(f'Saving data into {csv_output_path}')
    util.export_data_csv(ids_and_reviews, csv_output_path)

    print('Logging the last scraped date')
    util.log_etl_last_scraped_date(date_str)


# output_file_path = f'Output/Listings/Listing_{output_date_format}.csv'
if __name__ == '__main__':

    main(argv)


