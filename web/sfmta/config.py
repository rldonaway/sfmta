import os
basedir = os.path.abspath(os.path.dirname(__file__))

GOOGLE_MAPS_KEY = os.environ["GOOGLE_API_KEY"]

class Config(object):
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True


class ProductionConfig(Config):
    DEBUG = False


class StagingConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class DevelopmentConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
