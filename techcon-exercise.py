from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

# =====================================================================
# pyspark sessions automatically create a spark context, and sql context
dir(sc)
dir(sqlCtx)

# =====================================================================
# 1. working with dataframes

# load a json file directly into a dataframe
reviews = sqlCtx.jsonFile("/Users/elliottcordo/Projects/Caserta/spark-techcon15/data/yelp/yelp_academic_dataset_review.json")
# this could have been pulled from S3 or HDFS as well:
# reviews = sqlCtx.jsonFile("s3n://aws_id:aws_secret_key@caserta-public/yelp-academic-dataset/yelp_academic_dataset_review.json")

# how many partitions do we have?
reviews.rdd.getNumPartitions()

# lets repartition
reviews = reviews.repartition(2)

# let's see what the schema looks like
reviews.printSchema()

# take a sample of 5 rows
reviews.take(5)

# take a sample of 5 rows where users have voted a business "cool"
reviews.filter("votes.cool = 1").take(5)

# let's define a udf to use in our dataframe operation
calc_weight = udf(lambda votes, stars: stars if votes == 1 else 0, IntegerType())

# create a new dataframe using this udf
cool_business = reviews.select("business_id", "votes.cool", "stars", calc_weight("votes.cool", "stars").alias("cool_weight"))

cool_business.take(5)

# group by and sum
cool_business.select("business_id", "cool_weight").groupBy("business_id").sum("cool_weight").take(5)

# =====================================================================
# 2. lets add a second dataframe

business = sqlCtx.jsonFile("/Users/elliottcordo/Projects/Caserta/spark-techcon15/data/yelp/yelp_academic_dataset_business.json")

# lets cache business in memory
business.cache()

# run a count, this will cache the users df (lazy evaluation)
# run this count again when it completes and note the performance difference
business.count()

# reviews the schema
business.printSchema()

# join the data
joined = cool_business.join(business, reviews.business_id == business.business_id, "left_outer")

# group by city
joined.select(joined.city, joined.cool_weight).groupBy("city").sum("cool_weight").take(10)

# =====================================================================
# 3. finally some sql

# first register our dataframes as temporary tables:
business.registerTempTable("business")
reviews.registerTempTable("reviews")

sql = """
    select city, count(1) as cnt
    from reviews r
      join business b on b.business_id = r.business_id
    group by city """

# now a SQL statement!
sqlCtx.sql(sql).take(5)

# create a udf that can be leveraged in SQL
sqlCtx.registerFunction("good_or_bad", lambda x: 'good' if x >=3 else 'bad')


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
