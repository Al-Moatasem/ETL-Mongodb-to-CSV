# ETL MongoDB to CSV

This Python application was built to do an ETL job, in which it connects to MongoDB, selects data with a specified criteria, then loads the data into CSV files. The original requirement is subject to a technical evaluation, in which I had to use Talend Open Studio for Big Data, but due to technical issues (installing Talend, and configuring it to connect to MongoDB), I couldn't use it. So I decided to use Python instead.

### How to use?

1. Create a MongoDB cluster, then load sample dataset.

2. Download/clone this repository, rename `config-sample.json` into `config.json`, then fill the database credentials and the server name/IP.

3. Install required Python's modules

   ```bash
   pip install -r requirements.txt
   ```

4. Run the following command in the command line to execute the ETL jobs

   1. Running the application without specifying the date of `last_scraped`, the application will get the last extracted date and use the following date, if no last extracted date is available, the application will use the earliest date in `last_scraped` from the dataset.

      ```bash
      python app.py
      ```

   2. Specifying a date (YYYYMMDD)

      ```bash
      python app.py 20190211
      ```

5. The application will connect to the database, extract the data, create two CSV files, then log the used date in `etl_job_config.json` file.

   1. If the date used has no data, two empty files will created, and the log file will be updated.

   

### Performance

Due the limited scope of this project, there are few performance issue that I didn't handle

1. While connecting to the database and retrieving the data, the application runs two queries to select a specified set of columns per query, it would be more performant if a single query is used, then split the result set into two different tables.
2. The queries don't read the data in chunks.