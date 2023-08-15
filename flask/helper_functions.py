"""Moduel for fetching and enriching movies from the data source."""
from typing import List, Optional, Tuple

import pandas as pd
from config import Config
from imdb import IMDb

#interim solution: REMOVE!
#in the future this will call a spark app to return a Spark Dataframe
movie_titles: pd.DataFrame = pd.read_csv(Config.titles_path)

#get movie and additional information using imdbpy
ia = IMDb()


def get_title(movie_id: int) -> str:
    """Return title of a given movie Id (from the database).

    Args:
        movie_id (int): Id of the movie in the Database !NOT an imdb_id!

    Returns:
        str: Movie title.
    """

    # Provide functionality for pandas df or spark df.
    # pandas
    if isinstance(movie_titles, pd.DataFrame):
        title = list(movie_titles.loc[movie_titles["movieId"]== movie_id]["title"])[0]
    # spark
    else:
        title = movie_titles.filter(f"movieId == {movie_id}").collect()[0][1]
    return title


def get_image_url(title: str) -> str:
    """Use imdb to fetch a url for the cover_picture.

    Args:
        title (str):  Movie title

    Returns:
        str: Web url of the movie cover.
    """

    # get image url
    try:
        movie = ia.search_movie(title = title, results = 1)[0]
        cover_url = movie["full-size cover url"]
    # load default image if no image was found
    except (IndexError, KeyError):
        cover_url = (
            "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/"
            "No_image_available.svg/300px-No_image_available.svg.png"
        )

    return cover_url


def get_movie_from_pool(pool: Optional[pd.DataFrame] = None,
                        genre: Optional[List[str]] = None) -> Tuple[int, str, str]:
    """Draw a random movie from the dataframe. Supports Pandas and Spark dataframes.

    Args:
        pool (Optional[pd.DataFrame], optional): Pool from which to draw the movies from.
            If None all available movie data will be used. Defaults to None.
        genre (Optional[List[str]], optional): Specify the movie genre(s).
            Only supported when passing a Spark Dataframe at the moment.
            Defaults to None.

    Returns:
        Tuple[int, str, str]: movieid, title, cover_url
    """
    # default pool
    if pool is None:
        pool = movie_titles

    # Build genres string.
    if genre is None:
        genre = [""]
    genre = "|".join([x.title() for x in genre])

    # Spark support turn Spark --> Pandas dataframe and continue.
    if not isinstance(pool, pd.DataFrame):
        pool = pool.filter(f"genres like '%{genre}%'").toPandas()

    # Draw random movie.
    sample = pool.sample(1)

    # Extract neccesary information.
    # NOTE: Use list conversion here to avoid
    # serialization problems with numpy types.
    title = list(sample["title"])[0]
    movieid = list(sample["movieId"])[0]
    cover_url = get_image_url(title = title)

    return movieid , title , cover_url
