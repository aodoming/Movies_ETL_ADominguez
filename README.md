# DESCRIPTION
Using the Extract, Transform, Load Process to Create Data Pipelines. A Data Pipepline Transports Data from Source to Destination, and 
the ETL Process Creates the Pipelines and Transforms Data in the Process.
ETL the Wikipedia Movies JSON

### SITUATION/TASK

### APPROACH

### RESULTS
Clean good data readied for analysis

### THINGS LEARNED
* Create an ETL pipeline from raw data to a SQL database.
* Extract data from disparate sources using Python.
* Clean and transform data using Pandas.
* Use regular expressions to parse data and to transform text into numbers.
* Load data with PostgreSQL.

### SOFTWARE/SKILLS USED
PostgreSQL, Pandas, Python, ETL Pipelines, Regular Expressions


### 5 ASSUMPTIONS MADE
It's important to ensure that the following assumptions about the data are true: 

1) The function performs all transformation steps, with no errors.   
2) Data is sorted uniquely and in the same way in all input datasets.
   If this is not the case then the merge will be unable to match in some columns.
3) That the data is merged using a unique identifier such as "imdb_id" to avoid having duplicated rows in data. 
4) Some columns will have null values as a result of missing data.
5) The function performs all load steps with no errors, while existing data is removed while the load step reloads the data in SQL.
