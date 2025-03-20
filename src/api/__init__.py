from .SquidAPI import SquidAPI
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API-KEY")

__all__ = ["SquidAPI"]
