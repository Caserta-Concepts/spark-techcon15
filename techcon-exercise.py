
# coding: utf-8

# #Intro To Spark SQL

# In[ ]:

from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


# ##pyspark sessions automatically create a spark context, and sql context
# 

# In[ ]:

dir(sc)


# In[ ]:

dir(sqlCtx)


# ##1. Working with dataframes
# 

# load a json file directly into a dataframe

# In[ ]:

reviews = sqlCtx.jsonFile("/Users/elliottcordo/Projects/Caserta/spark-techcon15/data/yelp/yelp_academic_dataset_review.json")


# how many partitions do we have?

# In[ ]:

reviews.rdd.getNumPartitions()


# let's repartition

# In[ ]:

reviews = reviews.repartition(2)
reviews.rdd.getNumPartitions()


# let's see what the schema looks like

# In[ ]:

reviews.printSchema()


# take a sample of 5 rows

# In[ ]:

reviews.take(5)


# project only a few columns

# In[ ]:

reviews.select("business_id","stars", "votes.cool").take(5)


# apply a fitler

# In[ ]:

reviews.select("business_id", "stars", "votes.cool").filter("cool = 1").take(5)


# let's define a udf to use in our dataframe operation

# In[ ]:

calc_weight = udf(lambda votes, stars: stars if votes == 1 else 0, IntegerType())


# create a new dataframe using this udf

# In[ ]:

cool_business = reviews.select("business_id", "votes.cool", "stars", calc_weight("votes.cool", "stars").alias("cool_weight"))

cool_business.take(5)


# group by and sum

# In[ ]:

cool_business.select("business_id", "cool_weight").groupBy("business_id").sum("cool_weight").take(5)


# ##2. Lets add a second dataframe

# In[ ]:

business = sqlCtx.jsonFile("/Users/elliottcordo/Projects/Caserta/spark-techcon15/data/yelp/yelp_academic_dataset_business.json")


# lets cache business in memory

# In[ ]:

business.cache()


# reviews the schema

# In[ ]:

business.printSchema()


# join the data

# In[ ]:

joined = cool_business.join(business, reviews.business_id == business.business_id, "left_outer")


# group by city

# In[ ]:

joined.select(joined.city, joined.cool_weight).groupBy("city").sum("cool_weight").take(10)


# #3. Finally some sql

# first register our dataframes as temporary tables:

# In[ ]:

business.registerTempTable("business")
reviews.registerTempTable("reviews")


# now a SQL statement!

# In[ ]:

sql = """
    select city, count(1) as cnt
    from reviews r
      join business b on b.business_id = r.business_id
    group by city """

sqlCtx.sql(sql).take(5)


# create a udf that can be leveraged in SQL

# In[ ]:

sqlCtx.registerFunction("good_or_bad", lambda x: 'good' if x >=3 else 'bad')


# run SQL leverating this SQL statement

# In[ ]:

sql = """
    select name, cnt
    from (
      select b.name, count(1) as cnt
      from reviews r
        join business b on b.business_id = r.business_id
      where good_or_bad(r.stars) = "good"
      group by b.name) a
    order by cnt desc"""

sqlCtx.sql(sql).take(5)

