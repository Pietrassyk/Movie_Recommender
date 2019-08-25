class Config:
	num_ratings = 5
	default_user_id = 100011
	install_path = "/Users/pietrassyk/Dropbox/Coding/Projects/Movie_Recommender"
	spark_path = install_path + "/spark/spark-2.4.3-bin-hadoop2.7"
	spark_app_path = install_path + "/flask/scripts/Spark_App_Recommender.py"
	script_path = install_path + "/flask/scripts"
	ratings_path = script_path + "/data/ratings.csv"
	titles_path = script_path + '/data/movies.csv'

	num_recommendations = 5
