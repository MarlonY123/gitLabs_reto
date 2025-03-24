from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("series")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_series="dataset.csv"
    df_series = spark.read.csv(path_series,header=True,inferSchema=True)
    df_series.createOrReplaceTempView("series")
    query='DESCRIBE series'
    spark.sql(query).show(20)

    query="""SELECT name, genre, rating FROM series WHERE type=="TV" ORDER BY `rating`"""
    df_series_names = spark.sql(query)
    df_series_names.show(20)

    
    query='SELECT name, genre, episodes, rating FROM series WHERE episodes > 15  ORDER BY rating'
    df_serie_greater_15 = spark.sql(query)
    df_serie_greater_15.show(20)
    results = df_serie_greater_15.toJSON().collect()
    #print(results)
    df_serie_greater_15.write.mode("overwrite").json("results")
    #df_people_1903_1906.coalesce(1).write.json('results/data_merged.json')
    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    #query='SELECT sex,COUNT(sex) FROM people WHERE birth BETWEEN "1903-01-01" AND "1911-12-31" GROUP BY sex'
    #df_people_1903_1906_sex = spark.sql(query)
    #df_people_1903_1906_sex.show()
    spark.stop()
