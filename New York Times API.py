from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import requests
import time
import datetime
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("NYTimesData").getOrCreate()

# Define the time range for data retrieval
end_date = datetime.date.today()
start_date = end_date - relativedelta(days=1)
# Uncomment the line below for historic data retrieval (last 5 years)
# start_date = end_date - relativedelta(years=5)

# Generate a list of months in the specified range
months = [x.split(' ') for x in pd.date_range(start_date, end_date, freq='MS').strftime("%Y %-m").tolist()]

def request(date):
    """
    Send a request to the NYTimes API for a given date.
    """
    base_url = 'https://api.nytimes.com/svc/archive/v1/'
    url = f'{base_url}/{date[0]}/{date[1]}.json?api-key=3pY2fktl7re63egYxmrloMy5OsO65uBn'
    response = requests.get(url).json()
    time.sleep(6)
    return response

def validity(article, date):
    """
    Check if an article is valid based on specified criteria.
    """
    is_in_range = start_date < date < end_date
    has_headline = isinstance(article['headline'], dict) and 'main' in article['headline']
    return is_in_range and has_headline

def parse(article):
    """
    Parse the response from the NYTimes API and extract relevant information.
    """
    date = parse(article['pub_date']).date()
    if validity(article, date):
        return (
            date, article['headline']['main'], article.get('lead_paragraph', None),
            article.get('section_name', None), article['document_type'],
            article.get('type_of_material', None), article.get('abstract', None),
            article.get('snippet', None), article.get('word_count', None), article.get('source', None), 
            article.get('web_url', None)
        )

def extractdata(dates):
    """
    Retrieve data for each date in the specified range and return a combined DataFrame.
    """
    total = 0
    dfs = []
    for date in dates:
        response = request(date)
        articles = response.get('response', {}).get('docs', [])
        data = list(filter(None, [parse(article) for article in articles]))
        total += len(data)
        if data:
            rdd = spark.sparkContext.parallelize(data)
            df = rdd.toDF(['date', 'headline', 'lead_paragraph', 'section', 'doc_type', 'material_type', 'abstract', 'snippet', 'word_count', 'source', 'web_url'])
            dfs.append(df)
            

    # Union all DataFrames into a single DataFrame
    result_df = None
    if dfs:
        result_df = dfs[0]
        for df in dfs[1:]:
            result_df = result_df.union(df)
    
    return result_df

# Call extractdata with months and store the result in a DataFrame
result_dataframe = extractdata(months)

# Delta Lake path for storing the data
delta_path = "Tables/Nytimes"

# Write the PySpark DataFrame to Delta Lake in append mode
result_dataframe.write.format("delta").mode("append").save(delta_path)
