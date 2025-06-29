import configparser
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

# Read database connection string from secrets.ini
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'secrets.ini'))
db_url = config.get('database', 'url')

engine = create_engine(db_url)
pg_session_factory = sessionmaker(
    engine, Session, autocommit=False, autoflush=False, expire_on_commit=False
)
SessionFactory = scoped_session(pg_session_factory)


def get_session() -> Session:
    return SessionFactory()
