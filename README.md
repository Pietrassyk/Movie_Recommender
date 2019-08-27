# Movie Recommender
In this project I built a movie recommendation system based on the **Movielens Dataset** found <a href="https://grouplens.org/datasets/movielens/">here</a>.

My approach is using user based collaborative Filtering method.
The ratings approximation for unseen movies of a user are derived using pysparks AlternatingLeastSquares (ALS) model.
The model uses the following specifications:

```
als = ALS(maxIter=5,
          userCol='userId',
          itemCol='movieId',
          ratingCol='rating',
          seed=42,
          coldStartStrategy='drop',
          rank=50,
          regParam=0.01)
```
These model specifications where derived using Pyskpark's paramgrid builder and CrossValidation (not shown in this Repo).
The RMSE of this model lies at: `0.879`

You can test the model on my <a href="http://movie.pietrassyk.com">website</a>.

If the Server is down you can run the recommender locally.  Just follow the installation guide below.

## Installation
1. clone this repo onto your machine
2. Install the following packages using pip or pip3
    ```
    pip install flask
    pip install impdbpy
    ```
3. Make sure you also have installed on your machine
    * python 3
    * (python 2) if you want to run the stand alone spark version
    * Java 1.8
4. change the installation path in the ``flask/config.py`` file to the path you cloned this repo.

## Start the Server
1. Navigate to the root folder of this repo in the terminal and run `bash 01_start_webhook.sh`
<br>Now the flask Server is running.

2. Use the link provided in `/02_Vist_Localhost.webloc` <br>or type in the browser of your choice `0.0.0.0`

## Get Recommendations
Since your are a new user to the system you have to provide some initial ratings. Just press the buttons below the
cover according to your rating.
<br>If you dont know the movie press the `->`Button.
<br>Once you rated some movies (5 by default) give the system appx. 40 seconds to train the model and load your recommendations.

<img src="images/How_to.gif">

## Grab a drink and enjoy your movie


