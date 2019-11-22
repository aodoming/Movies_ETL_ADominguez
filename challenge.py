#!/usr/bin/env python
# coding: utf-8

# ## Extract the Wikipedia Movies JSON

# In[1]:


# Import Dependencies

import json
import pandas as pd
import numpy as np
import re               # Python module for regex

#Import SQL Modules
from sqlalchemy import create_engine
import psycopg2

# Ref Database Pswd
from config import db_password

import time


def movie_data_prep(wiki_movies_df, kaggle_movies_metadata, ratings):
    



    # In[8]:


    print('starting the movies_datA_prep function')
    # Create a New DF for wiki_movies
    wiki_movies_new_df = pd.DataFrame(wiki_movies )

    # Count the number if columns for this filtered data
    len(wiki_movies_new_df.columns)


    # 
    # #### Create a Function to Clean the Data, Part 1

    # In[9]:


    # Combine all the alternative titles to a dictionary

    sorted(wiki_movies_df.columns.tolist())


    # 
    # #### Handle the Alternative Titles

    # In[10]:


    def clean_movie(movie):      # First, write a simple function to make a copy of the movie and return it.
        movie = dict(movie)      #create a non-destructive copy
        
    # Step 1: Make an empty dict to hold all of the alternative titles.
        alt_titles = {}

    # Step 2: Loop through a list of all alternative title keys.combine alt titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            
    # Step 2a: Check if the current key exists in the movie object.
            if key in movie:
    # Step 2b: If so, remove the key-value pair and add to the alternative titles dictionary. Use pop()method
                alt_titles[key] = movie[key]
                movie.pop(key)
    # Step 3: After looping through every key, add the alternative titles dict to the movie object.
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        return movie


    # In[11]:


    # We can make a list of cleaned movies with a list comprehension:
    clean_movies = [clean_movie(movie) for movie in wiki_movies]


    # In[12]:


    # Set wiki_movies_df to be the DataFrame created from clean_movies, and print out a list of the columns.

    wiki_movies_df = pd.DataFrame(clean_movies)
    sorted(wiki_movies_df.columns.tolist())


    # 
    # #### Create a Function to Clean the Data, Part 2

    # In[13]:


    def clean_movie(movie):      # First, write a simple function to make a copy of the movie and return it.
        movie = dict(movie)      #create a non-destructive copy
        
    # Step 1: Make an empty dict to hold all of the alternative titles.
        alt_titles = {}

    # Step 2: Loop through a list of all alternative title keys.combine alt titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            
    # Step 2a: Check if the current key exists in the movie object.
            if key in movie:
    # Step 2b: If so, remove the key-value pair and add to the alternative titles dictionary. Use pop()method
                alt_titles[key] = movie[key]
                movie.pop(key)
    # Step 3: After looping through every key, add the alternative titles dict to the movie object.
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
            
            
            
    # # Consolidate columns with the same data into one column. Use the pop() method to change the name of a dictionary key
    # because pop() returns the value from the removed key-value pair.
    # Check if the key exists in a given movie record, so it will be helpful to make a small function inside clean_movie().

        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie


    # In[14]:


    # Rerun our list comprehension to clean wiki_movies and recreate wiki_movies_df.

    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)


    # 
    # ## Remove Duplicate Rows

    # In[15]:


    # (Mod 8.3.7) To ensure that there are no duplicate rows before we merge Wiki's IMDb ID with the Kaggle data,
    # need to extract the IMDb ID from IMDb link
    # Necessary to use the str.extract() method on Regular Expressions (regex) like so ("tt\d{7}")


    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    print('Progress, this is where we extract the imdb_id')
    print(len(wiki_movies_df))

    # drop any duplicates of IMDb IDs by using the drop_duplicates() method
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    print(len(wiki_movies_df))

    # How many new number of rows and how many rows were dropped
    wiki_movies_df.head()


    # 
    # #### Remove Mostly Null Columns

    # In[16]:


    # Remove columns that contain no useful data
    # To get the count of null values for each column is to use a list comprehension, as shown below.

    [[column,wiki_movies_df[column].isnull().sum()] for column in wiki_movies_df.columns]


    # In[17]:


    # Above output shows half the columns have more than 6,000 null values.
    # To remove the null values, make a list of columns that have less than 90% null values and use those to trim down our dataset.
    # Columns with less than 90% null values are the columns we want to keep. Select from dataframe
            # just have to tweak our list comprehension as so and put value in a variable 'wiki_columns_to_keep'

    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    wiki_movies_df.head()


    # 
    # ## Make a Plan to Convert and Parse the Data

    # In[18]:


    # Converting Box Office & Budget coloumns to numeric. 
    # Will be helpful to only look at rows where box office data is defined, so first make a data series that drops missing values 

    box_office = wiki_movies_df['Box office'].dropna() 
    len(box_office)             # this many rows have no nulls and have box office data


    # In[19]:


    #use the lambda function directly instead of using is_not_a_string():

    box_office[box_office.map(lambda x: type(x) != str)]


    # In[20]:


    # Output above shows a few of the data points are stored as lists. Concatenate list items into one string using a join() string
    # method, also applying a # simple space/seperator string as our joing char and apply the join() like so:

    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    box_office


    # 
    # ## Write Regular Expressions
    # 
    # 
    # 
    # 
    # 
    # ## Parse the Box Office Data

    #    ##### Create the First & Second Forms

    # In[21]:


    # Use regex to find out how many diff styles/forms like $122.7 million/bill or $7331,647 values are present in box office data
    form_one = r'\$\d+\.?\d*\s*[mb]illion'
    box_office.str.contains(form_one, flags=re.IGNORECASE).sum()


    # match the numbers of the second form, “$123,456,789.
    form_two = r'\$\d{1,3}(?:,\d{3})+'
    box_office.str.contains(form_two, flags=re.IGNORECASE).sum()


    #   #### Compare Values in Forms

    # In[22]:


    # (Ref: Mod8.3.10) check to see which values aren’t described by either forms

    matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)

            # "write this next line code in Pandas element-wise logical operators"
    # box_office[(not matches_form_one) and (not matches_form_two)]
        
            # Pandas element-wise logical operators,like so....
    box_office[~matches_form_one & ~matches_form_two]


    #   #### Fix Pattern Matches

    # In[23]:


    #  1. Some values have spaces in between the dollar sign and the number. To fix this pattern match Add \s*

    form_one = r'\$\s*\d+\.?\d*\s*[mb]illion'
    form_two = r'\$\s*\d{1,3}(?:,\d{3})+'


    # In[24]:


    # 2. Some values use a period as a thousands separator, not a comma.
    # Simply change form_two to allow for either a comma or period as a thousands separator, like so:

    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+'

    # add a negative lookahead group that looks ahead for “million” or “billion” after the number like 1.234 billion
    # and rejects the match if it finds those strings. Only looking to change raw numbers like $123.456.789.

    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'


    # In[25]:


    # 3. Some values are given as a range.
    # search for any string that starts with a dollar sign and ends with a hyphen, 
    # and then replace it with just a dollar sign using the replace() method.

    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)


    # In[26]:


    # 4. “Million” is sometimes misspelled as “millon.”
    # make the second “i” optional in our match string with a question mark as follows:

    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'


    # 
    # #### Extract and Convert the Box Office Values

    # In[27]:


    # Extract only the parts of the strings that match, with the str.extract() method. 

    box_office.str.extract(f'({form_one}|{form_two})')


    # In[28]:


    # Finally, convert all the strings to floats, multiply by the right amount, and return the value.

    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan


    # In[29]:


    #  parse the box office values to numeric values.

    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)


    # In[30]:


    # We no longer need the "Box Office"column, so we’ll just drop it:

    wiki_movies_df.drop('Box office', axis=1, inplace=True)


    # 
    # ## Parse Budget Data

    # In[31]:


    # how many budget values are in a different form. Create a budget variable 

    budget = wiki_movies_df['Budget'].dropna()


    # In[32]:


    # Convert any lists to strings:

    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)


    # In[33]:


    # Then remove any values between a dollar sign and a hyphen (for budgets given in ranges):

    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)


    # In[34]:


    # Use the same pattern matches that you created to parse the box office data, 
    # and apply them without modifications to the budget data.

    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    budget[~matches_form_one & ~matches_form_two]


    # In[35]:


    # Remove the citation references with the following:

    budget = budget.str.replace(r'\[\d+\]\s*', '')
    budget[~matches_form_one & ~matches_form_two]


    # In[36]:


    # Parse the budget values

    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)


    # In[37]:


    # drop the original Budget column.

    wiki_movies_df.drop('Budget', axis=1, inplace=True)


    # 
    # ## Parse Release Date

    # In[38]:


    # Make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings:

    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)


    # In[39]:


    # Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)

    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'


    # In[40]:


    # And then we can extract the dates with:

    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)


    # In[41]:


    # To parse the dates, we’ll use the built-in to_datetime() method in Pandas. Since there are different date formats,
    # set the infer_datetime_format option to True.

    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)


    # 
    # ## Parse Running Time

    # In[42]:


    # Make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings:

    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time


    # In[43]:


    # Ref(mod 8.3.11) Parse the 17 entries
    # match all of the hour + minute patterns with one regular expression pattern. Our pattern follows: "\d+\s*ho?u?r?s?\s*\d*"
    # extract digits & allow for both possible patterns. Add capture groups around the \d instances and add an alternating char

    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')


    # In[44]:


    # Code in above cell produces a DataFrame with all strings, Need to convert them to numeric values to avoid empty strings
    # Use the to_numeric() method and set the errors argument to 'coerce' . 
    # Coercing the errors will turn the empty strings into Not a Number (NaN), then we can use fillna() to change all the NaNs to zeros.

    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)


    # In[45]:


    # apply a function that will convert the hour capture groups and minute capture groups to minutes 
    # if the pure minutes capture group is zero, and save the output to wiki_movies_df:

    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)


    # In[46]:


    # Drop Running time from the dataset with the following code:

    wiki_movies_df.drop('Running time', axis=1, inplace=True)


    # 
    # ## Transform Phase /Clean Kaggle Data
    #             (Inspect, Plan, Execute)

    # In[47]:


    # Does all the columns have the correct data types

    kaggle_movies_metadata.dtypes

    # Covert each of the 6 columns into their rightful dtypes (video, release_date, budget, adiult, ID, popularity)
    # Before we convert the “adult” and “video” columns to "Boolean", we want to check that all the values are either True or False.

    kaggle_movies_metadata['adult'].value_counts()         # reveals some bad data


    # #### Remove Bad Data

    # In[48]:


    # remove bad/corrupted data from "kaggle_movies_metadata"

    kaggle_movies_metadata[~kaggle_movies_metadata['adult'].isin(['True','False'])]


    # In[49]:


    # Keep rows where adult is False, and then drop the “adult” column.

    kaggle_movies_metadata = kaggle_movies_metadata[kaggle_movies_metadata['adult'] == 'False'].drop('adult',axis='columns')


    # #### Convert Data Types

    # In[50]:


    # Convert the video "object dtypes" to boolean. Code to convert =   kaggle_movies_metadata['video'] == 'True'
    # Code creates the Boolean column we want. We just need to assign it back to video:

    kaggle_movies_metadata['video'] == 'True'
    kaggle_movies_metadata['video'] = kaggle_movies_metadata['video'] == 'True'


    # In[51]:


    # Convert columns: budget, id, & popularity into numeric dtypes.  Use the to_numeric() method from Pandas. 
    # Make sure the errors= argument is set to 'raise', so we’ll know if there’s any data that can’t be converted to numbers.

    kaggle_movies_metadata['budget'] = kaggle_movies_metadata['budget'].astype(int)
    kaggle_movies_metadata['id'] = pd.to_numeric(kaggle_movies_metadata['id'], errors='raise')
    kaggle_movies_metadata['popularity'] = pd.to_numeric(kaggle_movies_metadata['popularity'], errors='raise')


    # In[52]:


    # Convert release_date to datetime. Luckily, Pandas has a built-in function for that as well: to_datetime().

    kaggle_movies_metadata['release_date'] = pd.to_datetime(kaggle_movies_metadata['release_date'])


    # 
    # ## Reasonability Checks on Ratings Data

    # In[53]:


    # Use the info() method on the DataFrame, to check for ratings data
    # Set the null_counts option to True, because the ratings dataset has so many rows.

    ratings.info(null_counts=True)


    # In[54]:


    # Convert 'timestamp' dtype to a datetime data type to help store in SQL easier
    # Specify in to_datetime() that the origin is 'unix' and the time unit is seconds.

    pd.to_datetime(ratings['timestamp'], unit='s')

    print('Progress, this is whereconvert the timestamp column to a datetime')

    # In[55]:


    # Assign conversion to the timestamp column.

    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')


    # 
    # ## Merge Wikipedia and Kaggle Metadata
    # 
    # 
    # #### Compare Columns: Title

    # In[56]:


    # Print out a list of the columns so we can identify which ones are redundant.
    # We’ll use the suffixes parameter to make it easier to identify which table each column came from.

    movies_df = pd.merge(wiki_movies_df, kaggle_movies_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    movies_df.head(2)


    # In[57]:


    # Take a quick look at some of the titles.
    movies_df[['title_wiki','title_kaggle']]


    # In[58]:


    #  Look for the rows where the titles don’t match.

    movies_df[movies_df['title_wiki'] != movies_df['title_kaggle']][['title_wiki','title_kaggle']]


    # In[59]:


    # Confirm there aren’t any missing titles in the Kaggle data with the following code:

    movies_df[(movies_df['title_kaggle'] == '') | (movies_df['title_kaggle'].isnull())]


    # #### Compare Columns: Running time vs Runtime

    # In[60]:


    # Expect missing values froma merge. Scatter plots won’t show null values, so we need to fill them in with zeros 

    movies_df.fillna(0).plot(x='running_time', y='runtime', kind='scatter')


    # #### Compare Columns: Budget

    # In[61]:


    # Since budget_wiki and budget_kaggle are numeric, we’ll make another scatter plot to compare the values:

    movies_df.fillna(0).plot(x='budget_wiki',y='budget_kaggle', kind='scatter') 


    # #### Compare Columns: Box Office

    # In[62]:


    # The box_office and revenue columns are numeric, so we’ll make another scatter plot.
    movies_df.fillna(0).plot(x='box_office', y='revenue', kind='scatter')


    # In[63]:


    # Let’s look at the scatter plot for everything less than $1 billion in box_office.
    movies_df.fillna(0)[movies_df['box_office'] < 10**9].plot(x='box_office', y='revenue', kind='scatter')


    # #### Compare Columns: Release Date

    # In[64]:


    movies_df[['release_date_wiki','release_date_kaggle']].plot(x='release_date_wiki', y='release_date_kaggle', style='.')


    # In[65]:


    # Investigate that wild outlier around 2006. We’re just going to choose some rough cutoff dates to single out that one movie. 
    # Look for any movie whose release date according to Wikipedia is after 1996,
    # but whose release date according to Kaggle is before 1965

    movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')]


    # In[66]:


    # The Holiday in the Wikipedia data got merged with From Here to Eternity. We’ll have to drop that row from our DataFrame.
    # We’ll get the index of that row with the following:
    movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index


    # In[67]:


    #  drop the row

    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01')
                                        & (movies_df['release_date_kaggle'] < '1965-01-01')].index)


    # In[68]:


    # Now, see if there are any null values:
    movies_df[movies_df['release_date_wiki'].isnull()]    #Wikipedia data is missing release dates for 11 movies:
    movies_df.head(2)


    # #### Compare Columns: Language

    # In[69]:


    # For the language data from wiki, we’ll compare the value counts of each
    # First convert the lists in Language to tuples so that the value_counts() method will work.

    movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)


    # In[70]:


    # For the Kaggle data, there are no lists, so we can just run value_counts() on it.
    movies_df['original_language'].value_counts(dropna=False)


    # 
    # #### Compare Columns:  Production Companies

    # In[71]:


    # Compare Data from columns "Production company(s)" and "production_companies" from Wiki & Kaggle resp.

    movies_df[['Production company(s)','production_companies']]


    # #### Put It All Together

    # In[72]:


    # First, we’ll drop the title_wiki, release_date_wiki, Language, and Production company(s) columns.

    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)


    # In[73]:


    # To save a little time, we’ll make a function that fills in missing data for a column pair and then drops the redundant column.

    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis=1)
        df.drop(columns=wiki_column, inplace=True)


    # In[75]:


    # Run the function for the three column pairs that we decided to fill in zeros.

    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')


    # In[76]:


    # Check that there aren’t any columns with only one value, since that doesn’t really provide any information. 
    # Don’t forget, we need to convert lists to tuples for value_counts() to work.

    for col in movies_df.columns:
        lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
        value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
        num_values = len(value_counts)
        if num_values == 1:
            print(col)


    # In[77]:


    movies_df['video'].value_counts(dropna=False)            #Since it’s false for every row, we don’t need to include this column.


    # In[78]:


    # Reorder the columns 

    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                        'genres','original_language','overview','spoken_languages','Country',
                        'production_companies','production_countries','Distributor',
                        'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                        ]]


    # In[79]:


    # Finally, we need to rename the columns to be consistent.

    movies_df.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)


    # 
    # ## Transform and Merge Rating Data

    # In[80]:


    # Pivot this data so that movieId is the index, the columns will be all the rating values, 
    # and the rows will be the counts for each rating value.

    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                 .rename({'userId':'count'}, axis=1)                 .pivot(index='movieId',columns='rating', values='count')


    # In[81]:


    # We want to rename the columns so they’re easier to understand. We’ll prepend rating_ to each column with a list comprehension:

    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]


    # In[82]:


    # Use the left merge to join ratings counts onto movies_df

    print('Progress, this is where we merge')
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')


    # In[83]:


    # Fill the missing values resulting from a merge with zeros

    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    # ### Challenge

    # In[ ]:



    # In[ ]:



### WHEN READY TO SUBMIT, COMMENT-OUT YOUR COD EFROM HERE:
#---------------
# Import the Wikipedia JSON file by defining a variable file_dir for the directory that’s holding our data.

file_dir = r'C:\Users\aodom\OneDrive\Documents\Class_Folder_Berkeley/'
# Open a file in your directory, using an f-string 

f'{file_dir}filename'

# Using the with statement, open the Wikipedia JSON file to be read into the variable file, 
# and use json.load() to save the data to a new variable.
# Import kaggle movies metadata
kaggle_movies_metadata = pd.read_csv(f'{file_dir}kaggle_movies_metadata.csv', low_memory=False)
ratings = pd.read_csv(f'{file_dir}ratings.csv')

with open(f'{file_dir}//wikipedia-movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)
# Using list comprehension, modify our JSON data by restricting it to only those entries that have a 'director and 
# an 'IMDb' link.Filter expression we created and save that to an intermediate variable wiki_movies

wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
# len(wiki_movies)
wiki_movies_df = pd.DataFrame(wiki_movies_raw)
movie_data_prep(wiki_movies_df, kaggle_movies_metadata, ratings)
#TO HERE 
# #---------------