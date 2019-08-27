#imports
import pandas as pd
from imdb import IMDb #use: pip install imdbpy to install

from config import Config

#interim solution: REMOVE!
#in the future this will call a spark app to return a Spark Dataframe
movie_titles = pd.read_csv(Config.titles_path)


#get movie and additional information using imdbpy
ia = IMDb()

def get_title(movie_id):
    """Return title of a given movie Id (from the database)
    Params
    --------
    movie_id : int
        Id of the movie in the Database !NOT and imdb_id!
    Returns
    --------
    title : str
        Movie title
    """

    ##provide functionality for pandas df or spark df
    #pandas
    if isinstance(movie_titles,pd.core.frame.DataFrame):
        title = list(movie_titles.loc[movie_titles["movieId"]== movie_id]["title"])[0]
    #spark
    else:
        title = movie_titles.filter(f"movieId == {movie_id}").collect()[0][1]
    return title

def get_image_url(title):
    """Use imdb to fetch a url for the cover_picture
    Params
    --------
    title : str
        Movie title
    Returns
    --------
    cover_url : str
        Web url of the movie cover"""

    #get image url
    try:
        movie = ia.search_movie(title = title, results = 1)[0]
        cover_url = movie["full-size cover url"]
    #load default image if no image was found
    except:
        cover_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/300px-No_image_available.svg.png"
    
    return cover_url

def get_movie_from_pool(pool = None, genre = [""]):
    """Draw a random movie from the dataframe. Supports Pandas and Spark dataframes
    Params
    --------
    pool : Pandas or Spark dataframe
        Pool from which to draw the movies from. Can be a subset of users or exclude the movies the current user has already seen
    genre : list of strings
        specify the movie genre(s) [only supported in Spark at the moment]
    Returns
    --------
    movieId : int
    title : str
    cover_url : str 
    """
    
    #build genres string
    genre = [x.title() for x in genre]
    genre = "|".join(genre)

    #default pool
    if pool == None:
        pool = movie_titles

    #Spark support turn Spark --> Pandas dataframe and continue
    if not isinstance(pool,pd.core.frame.DataFrame):
        pool = pool.filter(f"genres like '%{genre}%'").toPandas()

    #draw random movie
    sample = pool.sample(1)

    #extract neccesary information
    title = list(sample["title"])[0]
    movieId = list(sample["movieId"])[0]
    cover_url = get_image_url(title = title)

    return movieId , title , cover_url
    