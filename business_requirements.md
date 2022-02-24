# Business Requirements
1. Create an ETL job that produces one .csv (comma separated and text delimited) file per last_scraped date
    * The file name must be in the format Listings_YYYYMMDD.csv
    * Each file must contain all the listings that were last scraped on a day and include all fields except ‘reviews’
    * Any fields that are arrays or objects (eg. ‘amenities’, ‘host’) must be stored as JSON formatted strings with line breaks removed
    * The ETL job must be parameterised so that it can be run on a specific date and only produce an output file for the specified date
    * If an output file already exists for the specified date, the file must be overwritten
    * If no date is specified when the ETL runs, it must look at the latest output file that was produced and generate an output file for the following day
    * If no date is specified and no output files exist, the ETL must produce a file for the first last_scraped date in the dataset

2. Create an ETL job that produces one .csv file per review date
    * The file name must be in the format Reviews_YYYYMMDD.csv
    * Each file must contain all reviews on a specific date and also include the associated listing’s ‘_id’ field.
    * The ETL job must be parameterised so that it can be run on a specific date and only produce an output file for the specified date
    * If an output file already exists for the specified date, the file must be overwritten
    * If no date is specified when the ETL runs, it must look at the latest output file that was produced and generate an output file for the following day
    * If no date is specified and no output files exist, the ETL must produce a file for the first review date in the datase