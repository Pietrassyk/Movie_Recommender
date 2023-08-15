"""Flask App."""
import json
import subprocess
from subprocess import PIPE

from config import Config
from helper_functions import get_image_url, get_movie_from_pool
from schemas import RatingSchema

from flask import (Flask, make_response, redirect, render_template, request,
                   url_for)

app = Flask(__name__)

# TODO: Actually use pydantic schemas.
def build_request_json(user_id: int, new_ratings: RatingSchema) -> str:
    """Create a json that is used to be passed to the Spark Recommendation Model.

    Args:
        user_id (int): Id for new or existing User in the Database.
        new_ratings (RatingSchema): New ratings.

    Returns:
        str: Json string from a valid RecommenderRequest.
    """
    request_dict = {"user_id": user_id,
                    "new_ratings" : new_ratings ,
                    "ratings_path" : Config.ratings_path,
                    "titles_path" : Config.titles_path,
                    "num_recommendations" : Config.num_recommendations}
    return json.dumps(request_dict)


@app.route('/', methods = ["GET"])
def show_movie_specs():
    """Draw movie from Database and render title + cover, then wait for user input"""
    # get movie
    movieid, title, cover_url = get_movie_from_pool()
    res = make_response(render_template("movie_selection.html", title=title, cover_url=cover_url))

    #store current movie in a cookie so that the information is accessable outside this route
    movie_dict = dict(zip(["movieId", "title", "cover_url"], [movieid, title, cover_url]))
    movie_json = json.dumps(movie_dict)
    res.set_cookie("curr_movie", movie_json)

    return res


@app.route('/', methods = ["POST"])
def store_rating():
    """Create response to the users movie rating."""

    # Create user_id from IP adress
    ip_ = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    # Limit ip-address to 6 digits so java can still handle it.
    user_id = int(ip_.replace(".", "")[-6:])

    res = make_response(redirect("/#rating"))

    # Read and set cookies
    cookie = request.cookies
    ratings = int(cookie.get("rated_movies", 0))  # Keeps track of the number of rated movies.

    # Select answer to incoming user input
    resp = request.form["rating"]

    # skip movie
    if resp.lower()== "n":
        return redirect("/#rating")

    # append movie to new ratings
    curr_movie = json.loads(cookie.get("curr_movie"))
    new_entry = {
        ratings: {
            "userId": user_id,
            "movieId": curr_movie["movieId"],
            "rating": resp
        }
    }
    new_ratings = json.loads(cookie.get("new_ratings", "{}"))
    new_ratings.update(new_entry)
    res.set_cookie("new_ratings", json.dumps(new_ratings))
    res.set_cookie("rated_movies", value=f"{ratings+1}")

    # Repeat untill configured number of ratings is reached.
    if ratings + 1 < Config.num_ratings:
        return res

    # When collected enough ratings proceed to recommendation.
    req_json = build_request_json(user_id, new_ratings)
    res_recommendation = make_response(redirect(url_for("show_recommendations")))
    # This cookie will be passed as request for the spark model.
    res_recommendation.set_cookie("temp_json", req_json)

    # Delete ratings counter cookie
    res_recommendation.set_cookie("rated_movies", "", expires = 0)

    return res_recommendation


@app.route("/Recommendations", methods = ["GET"])
def show_recommendations():
    """Request recommendations from Spark model and render results"""

    # Collect necessary inforamtion from cookies and config.
    temp_json = request.cookies.get("temp_json")
    spark_path = Config.spark_path
    spark_app = Config.spark_app_path

    # Run the actual spark model.
    # This step is crucial and has to be adjusted to the running environment (EC2 or local).
    run_spark_app = subprocess.Popen(
        ["./bin/spark-submit", spark_app , temp_json],
        cwd=spark_path,
        stdout=PIPE
    )
    # Recommendation results.
    recommendations_json = json.loads(run_spark_app.stdout.read())

    # Convert results into json for the output page.
    movie_ids = [recommendations_json[key] for key in recommendations_json.keys()]
    cover_urls = dict(zip(recommendations_json.keys(),[get_image_url(id_) for id_ in movie_ids]))
    res = make_response(
        render_template(
            "recommendations.html",
            cont=recommendations_json,
            cover_urls=cover_urls
        )
    )

    # Delete unneccesary cokies.
    res.set_cookie("temp_json", "", expires = 0)

    return res

#run flask app
if __name__ == '__main__':
    app.run(host = "0.0.0.0", port = 80,debug = True)
