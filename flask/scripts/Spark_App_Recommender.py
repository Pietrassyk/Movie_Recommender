# import necessary libraries
import sys
import json

from pyspark import  SparkConf
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession

"""This app will train a Spark ALS model and return a number if movie recommendations. To run, call this app pass a json holding the following parameters:
{
"user_id" : <int>,
"new_ratings" : {
                  <int> : {"userId": <int> , "movieId":<int> , "rating": <int>},
                  <int> : {"userId": <int> , "movieId":<int> , "rating": <int>},
                  <...>
                },
"ratings_path" : <absolute path to ratings -> see config>,
"titles_path" : <absolute path to titles -> see config>,
"num_recommendations" : <int -> see config>
}"""

#build Spark App
if __name__ == "__main__":

    def name_retriever(movie_id,movie_title_df):
        """Fetch movie name from Id
        Params
        --------
        movie_id : int
        movie_title_df : Spark dataframe
          Dataframe containing the movie titles and Ids
        Returns
        --------
        title : str
        """
        title = movie_title_df.filter("movieId == {}".format(movie_id)).collect()[0][1]
        return title
    
    def new_user_recs(user_id, new_ratings, rating_df, movie_title_df, num_recs):
        """Adds new recommendations to the excisting dataframe then trains the ALS model and returns recommendations
        Params
        --------
        user_id : int
          Id of the User that gave the ratings and needs recommendations
        new_ratings : list
          List of tuples with the new ratings like this
            [(<userId : int> , <movieId : int> , <rating : float>)]
        rating_df : Spark dataframe
        movie_title_df : Spark dataframe
        num_recs : int
          Number of desired recommendations
        Returns
        --------
        out_list : list
          List of strings with the recommended movie titles
        """

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

    #check for correct number of arguments
    if len(sys.argv) !=2:
      print("""Usage: full_path/Spark_App_Recommender.py
                       <request_json as string>
                       """)
      exit(-1)

    #build Session
    spark = SparkSession\
            .builder\
            .appName("Spark_App_Recommender").config("spark.driver.host","localhost")\
            .getOrCreate()

    #extract request parameters
    req_json = json.loads(sys.argv[1])
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
    movie_titles = spark.read.csv(titles_csv_path, header = True)
    
    #data cleaning
    try:
      movie_ratings = movie_ratings.drop("timestamp")
    except:
      pass
    
    #get recommendations and convert into json
    out_list = new_user_recs(user_id, new_ratings, movie_ratings, movie_titles, num_recs)
    app_return = json.dumps(dict(enumerate(out_list)))

    #write results to the command line or subprocess listener
    sys.stdout.write(app_return)


    

