import os
import sys

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")
sys.path.insert(0, os.path.abspath(DAGS_FOLDER))
