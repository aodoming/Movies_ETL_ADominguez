# Movies_ETL_ADominguez
ETL the Wikipedia Movies JSON

DOCUMENT 5 ASSUMPTIONS


It's important to make sure that the following assumptions about the data are true: 

    1) The function performs all transformation steps, with no errors. 
    
    2) Data is sorted uniquely and in the same way in all input datasets.
       If this is not the case then the merge will be unable to match in some columns. 
       
    3) That the data is merged using a unique identifier such as "imdb_id" to avoid having duplicated rows in data.
    
    4) Some columns will have null values as a result of missing data.
    
    5) The function performs all load steps with no errors, while existing data is removed while the load step reloads the data in SQL.
