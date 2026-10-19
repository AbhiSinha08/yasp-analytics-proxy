import os
from datetime import timedelta

from flask import session

SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_METADATA_DATABASE_URI"]
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}

SESSION_COOKIE_NAME = "yasp_superset_session"
PERMANENT_SESSION_LIFETIME = timedelta(days=365)
SESSION_REFRESH_EACH_REQUEST = True


def configure_local_sessions(app):
    @app.before_request
    def mark_session_permanent():
        session.permanent = True


FLASK_APP_MUTATOR = configure_local_sessions
