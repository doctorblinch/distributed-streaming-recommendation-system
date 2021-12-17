import json
from dataclasses import dataclass


@dataclass
class RecommendationRequest():
    user_id: int
    movie_id: int
    timestamp: str

    def to_message(self):
        message = {
            "userId": self.user_id,
            "movieId": self.movie_id,
            "timestampGenerator": self.timestamp,
        }
        return json.dumps(message)
