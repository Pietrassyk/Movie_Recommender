class Config:
	"""Class holding ths projects conigurations"""
	
	num_ratings = 5 #ratings a user have to give before receiving recommendations
	default_user_id = 100011

	install_path = "/Users/pietrassyk/Dropbox/Coding/Projects/Movie_Recommender" #addept to your system
	
	spark_path = install_path + "/spark/spark-2.4.3-bin-hadoop2.7" #loaction of the spark environment: change if you use a different installation
	spark_app_path = install_path + "/flask/scripts/Spark_App_Recommender.py"
	script_path = install_path + "/flask/scripts"
	ratings_path = script_path + "/data/ratings.csv"
	titles_path = script_path + '/data/movies.csv'

	num_recommendations = 5
