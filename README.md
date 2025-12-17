# dlt-reliable-scalable-data-pipeline
Data pipeline (reliability, scale, schema management, incremental loads, multiple destinations) with dlt

# *Project Overview*
Build Data Pipeline from public datasets into MongoDB using dlt(data load tool). It Python library that makes it easy to build reliable, maintainable, and production-ready data pipelines in just a few lines of code.
![]()

# *Problem To Be Solved*

# *Business Impact*

# *Business Leverage*

# *Project Flow*
1. Create virtual environment & activate it
   ```bash
   ❯ python3 -n venv mongo_dlt
   ❯ cd mongo_dlt
   ❯ source bin/activate
   (mongo_dlt) ❯
   ```
2. Write python3 code for dlt operation load data from public datasets into mongodb
   ```bash
   (mongo_dlt) ❯ lvim rest_api_mongo.py
   ```
3. Run the dlt within python code
   ```bash
   (mongo_dlt) ❯ python3 rest_api_mongo.py
   Downloading dataset...
   Extracting Excel file...
   Running dlt pipeline (≈20–40 seconds)...
   Pipeline online_retail_to_mongo load step completed in 1 minute and 31.15 seconds
   1 load package(s) were loaded to destination duckdb and into dataset retail_data
   The duckdb destination used duckdb:////home/mulyo/Learning/mongo_dlt/online_retail_to_mongo.duckdb location to store data
   Load package 1765963171.9467556 is LOADED and contains no failed jobs
   Inserted 541,909 documents into MongoDB
   All done! 541,909 transactions are now in MongoDB.
   ```
5. Checking the result on mongodb
   ```mongosh
   ❯ mongosh -u rootuser -p
   my_api_db> show collections
   online_retail
   my_api_db> db.online_retail.countDocuments()
   541909

   my_api_db> db.online_retail.find().limit(1)
   [
     {
       _id: ObjectId('69427688a544d31be2fc6f58'),
       invoice_no: 536365,
       stock_code: '85123A',
       description: 'WHITE HANGING HEART T-LIGHT HOLDER',
       quantity: 6,
       invoice_date: ISODate('2010-12-01T08:26:00.000Z'),
       unit_price: 2.55,
       customer_id: 17850,
       country: 'United Kingdom',
       _dlt_load_id: '1765963171.9467556',
       _dlt_id: 'gPhSsozVRmH2yQ',
       invoice_no__v_text: null
     }
   ]   
   ```

# *Assumption*
1. dlt & other python library installed on system, recomended on virtual environment
   ```bash
   ❯ pip install "dlt[duckdb,filesystem]" pandas openpyxl pymongo requests
   ❯ dlt --version
    dlt 1.20.0
   ```
2. MongoDB database ready for database or datawarehouse
   ```bash
   ❯ mongosh --version
    2.5.10 
   ``` 
