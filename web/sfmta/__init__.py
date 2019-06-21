import os

from flask import Flask
from flask import render_template

from sfmta.db import db_session

def create_app(test_config=None):
    """Create and configure an instance of the Flask application."""
    app = Flask(__name__, instance_relative_config=True)

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile("config.py", silent=True)
    else:
        # load the test config if passed in
        app.config.update(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route("/hello")
    def hello():
        return "Hello, World!"

    @app.route("/")
    def home():
        return render_template("index.html")

    # register the database commands
    from sfmta import db

    db.init_db()

    # apply the blueprints to the app
    from sfmta import reference

    app.register_blueprint(reference.bp)

    # make url_for('index') == url_for('blog.index')
    # in another app, you might define a separate main index here with
    # app.route, while giving the blog blueprint a url_prefix, but for
    # the tutorial the blog will be the main index
    app.add_url_rule("/", endpoint="index")

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db_session.remove()

    return app


