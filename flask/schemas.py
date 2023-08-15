"""API schemas."""
# pylint: disable=missing-class-docstring
from pydantic import BaseModel


class RatingSchema(BaseModel):
    user_id: int
    movie_id: int
    rating: float


class RecommenderRequest(BaseModel):
    user_id: int
    new_ratings: RatingSchema
    ratings_path: str
    titles_path: str
    num_recommendations: int
