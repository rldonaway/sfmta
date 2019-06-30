from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os

db_url = os.environ['DATABASE_URL']
db_user = os.environ['DATABASE_USER']
db_password = os.environ['DATABASE_PASSWD']

# 'postgresql://insight:passwd@ip-10-0-0-24.us-west-2.compute.internal:5217/sfmta'
engine = create_engine('postgresql://{}:{}@{}'.format(db_user, db_password, db_url))
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


def init_db():
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    import sfmta.models
    Base.metadata.create_all(bind=engine)

