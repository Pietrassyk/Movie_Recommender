# import necessary libraries
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("ALSExample").config("spark.driver.host","localhost")\
        .getOrCreate()

# read in the dataset into pyspark DataFrame
movie_ratings = spark.read.csv("./data/ratings.csv",header = 'true' , inferSchema = 'true')

#data cleaning
movie_ratings = movie_ratings.drop("timestamp")

#build the model
from pyspark.ml.recommendation import ALS
# split into training and testing sets
(train, test) = movie_ratings.randomSplit((0.8,0.2),42 )
print(train.count(), test.count())

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter = 5,
          rank = 10,
          userCol = 'userId',
          itemCol = 'movieId',
          ratingCol = 'rating',
          seed = 42,
          coldStartStrategy = 'drop',
          regParam = 0.01)
# fit the ALS model to the training set
model = als.fit(train)

movie_titles = spark.read.csv('./data/movies.csv', header = True)

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
    print("\n".join(out_list))
    return out_list

def movie_rater(movie_titles,num, genre=None, userId = 1000):
    if genre == None:
        genre = ""
    pool = movie_titles.filter(f"genres like '%{genre.title()}%'").toPandas()
    selection = pool.sample(num).reset_index(drop = True)
    ratings =[]
    i = 0
    while i <= max(selection.index):
        movie = selection.loc[i,:]
        print("")
        print(movie)
        rating = input("How do you rate this movie on a scale of 1-5, press n if you have not seen:")
        if rating.lower() == "n":
            selection = selection.append(pool.query("movieId not in @selection.movieId").sample(1), ignore_index = True)
        elif float(rating)<=5:
            ratings.append((int(userId),int(movie.movieId),float(rating)))
            #ratings.append({"userId" : userId,
            #                "movieId": movie.movieId,
            #                "rating":float(rating)})
        else:
            print("---------")
            print("Input incorrect, please try again")
            print("---------")
            i -=1
        i += 1
    return ratings