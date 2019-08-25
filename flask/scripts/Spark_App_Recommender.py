# import necessary libraries
import sys
import json
from pyspark import  SparkConf
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
if __name__ == "__main__":

    def name_retriever(movie_id,movie_title_df):
        title = movie_title_df.filter(f"movieId == {movie_id}").collect()[0][1]
        return title
    
    def new_user_recs(user_id, new_ratings, rating_df, movie_title_df, num_recs):
        # turn the new_recommendations list into a spark DataFrame
        new_rates = spark.createDataFrame(new_ratings)
        # combine the new ratings df with the rating_df
        rating_df = rating_df.union(new_rates)
    
        # create an ALS model and fit it
        als = ALS(maxIter=5,
                  userCol='userId',
                  itemCol='movieId',
                  ratingCol='rating',
                  seed=42,
                  coldStartStrategy='drop',
                  rank=50,
                  regParam=0.01)
        model = als.fit(rating_df)
        # make recommendations for all users using the recommendForAllUsers method
        recommendations = model.recommendForAllUsers(num_recs)
        user_recommends = recommendations.filter(recommendations.userId == user_id)
        recs_list = user_recommends.collect()[0]['recommendations']
        # get recommendations specifically for the new user that has been added to the DataFrame
        out_list = [name_retriever(row[0], movie_title_df) for row in recs_list]
        return out_list

    if len(sys.argv) !=2:
      print("""Usage: full_path/Spark_App_Recommender.py
                       <request_json as string>
                       """ , file = sys.stderr)
      exit(-1)

    spark = SparkSession\
            .builder\
            .appName("Spark_App_Recommender").config("spark.driver.host","localhost")\
            .getOrCreate()

    req_json = json.loads(sys.argv[1])
    
    #extract request parameters
    user_id = int(req_json["user_id"])

    new_ratings = [(int(req_json["new_ratings"][entry]["userId"]), 
                    int(req_json["new_ratings"][entry]["movieId"]), 
                    float(req_json["new_ratings"][entry]["rating"])) 
                       for entry in req_json["new_ratings"].keys()]

    ratings_csv_path = req_json["ratings_path"]
    titles_csv_path = req_json["titles_path"]
    num_recs = int(req_json["num_recommendations"])

    # read in the dataset into pyspark DataFrame
    movie_ratings = spark.read.csv(ratings_csv_path,header = 'true' , inferSchema = 'true')
    
    #data cleaning
    movie_ratings = movie_ratings.drop("timestamp")
    
    movie_titles = spark.read.csv(titles_csv_path, header = True)

    out_list = new_user_recs(user_id, new_ratings, movie_ratings, movie_titles, num_recs)
    app_return = json.dumps(dict(enumerate(out_list)))
    sys.stdout.write(app_return)


    

