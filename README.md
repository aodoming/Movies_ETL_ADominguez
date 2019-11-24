# Movies_ETL_ADominguez
ETL the Wikipedia Movies JSON

DOCUMENT 5 ASSUMPTIONS
It's important to make sure that the following assumptions about the data are true: 
    1) It makes the assumption that data is sorted uniquely and in the same way in all input datasets.
       If this is not the case then the merge will be unable to match some variants. 
       
    2) That the data is merged using a unique identifier "imdb_id" to avoid having duplicated rows and data
    
    3) Some columns will have null values as a result of missing data
    
    4) 
