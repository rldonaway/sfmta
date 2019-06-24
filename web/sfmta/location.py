import re
import sys
from flask import Blueprint
from flask import render_template
from flask import flash
from flask import redirect
from flask import request
from flask import url_for

from sfmta.db import db_session

from sfmta.models import Vehicle

bp = Blueprint("location", __name__)
dt_regexp = "^201\d{1}-[01]\d-[012]\d [012]\d:[0-5]\d:[0-5]\d$"

@bp.route("/locationFunctions", methods=("GET", "POST"))
def location_functions():
    if request.method == "POST":
        datetime = request.form["datetime"]
        timewindow = request.form["timewindow"]
        error = None

        if not datetime:
            error = "Date and time are required."

        if not re.search(dt_regexp, datetime):
            error = "Date and time must be in the format YYYY-MM-DD hh:mm:ss and be between 2013-01-01 and 2015-12-28."

        if not timewindow:
            error = "Time window is required."

        try:
            timewindow_int = int(timewindow)
            if not(1 <= timewindow_int <= 120):
                error = "Time window must be between 1 and 120."
        except ValueError:
            error = 'Please enter an integer value for the time window.'

        if error is not None:
            flash(error)
        else:
            function_resultproxy = db_session.execute("select list_stopped_vehicles(:datetime, :window)",
                {"datetime": datetime, "window": timewindow}
            )
            vehicles = []
            db_vehicles = function_resultproxy.fetchall()
            for row in db_vehicles:
                d = dict(row.items())
                rawdata = d["list_stopped_vehicles"].split(",")
                vehicle = []
                vehicle.append(rawdata[0][1:])
                vehicle.append('' if rawdata[1] == "\"\"" else rawdata[1])
                vehicle.append(rawdata[2][1:19])
                vehicle.append(round(float(rawdata[3])/60, 1))
                vehicle.append(rawdata[4])
                vehicle.append(rawdata[5][:-1])
                vehicles.append(vehicle)
            function_resultproxy.close()
            return render_template("location/index.html", vehicles=vehicles)

    return render_template("location/index.html")

@bp.route("/time")
def time():
    return render_template("location/time_stopped.html")


# def delete(id):
#     """Delete a post.
#
#     Ensures that the post exists and that the logged in user is the
#     author of the post.
#     """
#     get_post(id)
#     db = get_db()
#     db.execute("DELETE FROM post WHERE id = ?", (id,))
#     db.commit()
#     return redirect(url_for("blog.index"))
