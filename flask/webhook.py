from flask import Flask , render_template , request , redirect , make_response , url_for
from helper_functions import get_movie_from_pool ,get_image_url, get_title
from config import Config
import subprocess
from subprocess import PIPE
import json

app = Flask(__name__)

def build_request_json(user_id, new_ratings):
    request_dict = {"user_id": user_id,
                    "new_ratings" : new_ratings ,
                    "ratings_path" : Config.ratings_path,
                    "titles_path" : Config.titles_path,
                    "num_recommendations" : Config.num_recommendations}
    return json.dumps(request_dict)

@app.route('/Movie', methods = ["GET"])
def show_movie_specs():
    movieId , title , cover_url = get_movie_from_pool()
    res = make_response(render_template("movie_selection.html", title=title, cover_url = cover_url))
    movie_dict = dict(zip(["movieId", "title", "cover_url"], [movieId, title, cover_url]))
    movie_json = json.dumps(movie_dict)
    res.set_cookie("curr_movie",movie_json)
    return res

@app.route('/Movie', methods = ["POST"])
def store_rating():
    #DEBUG HERE
    user_id = Config.default_user_id
    #END

    res = make_response(redirect("/Movie#rating"))
    cookie = request.cookies
    ratings = int(cookie.get("rated_movies",0))

    resp = request.form["rating"]
    
    if resp.lower()== "n":
        return redirect("/Movie#rating")
    else:
        curr_movie = json.loads(cookie.get("curr_movie"))
        new_entry = {ratings : {"userId": user_id, 
                     "movieId": curr_movie["movieId"],
                     "rating": resp}}
        new_ratings = json.loads(cookie.get("new_ratings","{}"))
        new_ratings.update(new_entry)
        res.set_cookie("new_ratings", json.dumps(new_ratings))
        res.set_cookie("rated_movies", 
                       value = f"{ratings+1}")

        if ratings+1 < Config.num_ratings:
            return res
    req_json = build_request_json(user_id, new_ratings)
    res_recommendation = make_response(redirect(url_for("show_recommendations")))
    res_recommendation.set_cookie("temp_json", req_json)

    return res_recommendation

@app.route("/Recommendations", methods = ["GET"])
def show_recommendations():
    temp_json = request.cookies.get("temp_json")
    spark_path = Config.spark_path
    spark_app = Config.spark_app_path
    run_spark_app = subprocess.Popen(["sudo","./bin/spark-submit", spark_app , temp_json] , cwd = spark_path, stdout = PIPE)
    recommendations_json = json.loads(run_spark_app.stdout.read())
    #DEBUG
    #with open("temp.json","w") as f:
    #    f.write(json.dumps(recommendations_json))
    #END
    movie_ids = [recommendations_json[key] for key in recommendations_json.keys()]
    cover_urls = dict(zip(recommendations_json.keys(),[get_image_url(id_) for id_ in movie_ids]))
    return render_template("recommendations.html", cont = recommendations_json , cover_urls = cover_urls)

if __name__ == '__main__':
    app.run(host = "0.0.0.0", port = 80,debug = True)
