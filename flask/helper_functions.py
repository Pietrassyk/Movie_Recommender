from imdb import IMDb
import pandas as pd
from config import Config


#interim solution: REMOVE!
movie_titles = pd.read_csv(Config.titles_path)


#get movie and additional information using imdbpy
ia = IMDb()

def get_title(movie_id):
    if isinstance(movie_titles,pd.core.frame.DataFrame):
        title = list(movie_titles.loc[movie_titles["movieId"]== movie_id]["title"])[0]
    else:
        title = movie_titles.filter(f"movieId == {movie_id}").collect()[0][1]
    return title

def get_image_url(title):
    try:
        movie = ia.search_movie(title = title, results = 1)[0]
        cover_url = movie["full-size cover url"]
    except:
        cover_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/300px-No_image_available.svg.png"
    return cover_url

def get_movie_from_pool(pool = None):
    if pool == None:
        pool = movie_titles
    if not isinstance(pool,pd.core.frame.DataFrame):
        pool = pool.filter(f"genres like '%{genre.title()}%'").toPandas()
    sample = pool.sample(1)
    title = list(sample["title"])[0]
    movieId = list(sample["movieId"])[0]
    cover_url = get_image_url(title = title)
    # try:
    #     movie = ia.search_movie(title = title, results = 1)[0]
    #     cover_url = movie["full-size cover url"]
    # except:
    #     print(f"Could find movie with title: {title}")
    #     cover_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/300px-No_image_available.svg.png"
    return movieId , title , cover_url
    