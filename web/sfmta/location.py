import sys

from flask import Blueprint
from flask import render_template
from flask import flash
from flask import redirect
from flask import request
from flask import url_for

from sfmta.models import Vehicle, VehicleStop
from sfmta.location_db import query_for_vehicles, query_for_vehicles_hist
from sfmta.location_valid import validate_date_input, validate_time_input
from sfmta.config import GOOGLE_MAPS_KEY

bp = Blueprint("location", __name__)


@bp.route("/stoppedDate", methods=("GET", "POST"))
def stopped_by_date():
    if request.method == "POST":
        datetime = request.form["datetime"]
        time_window = request.form["timewindow"]
        error = validate_date_input(datetime, time_window)

        if error is not None:
            flash(error)
        else:
            vehicles = query_for_vehicles(datetime, time_window, 500)
            return render_template("location/date_stopped.html", vehicles=vehicles)

    return render_template("location/date_stopped.html")


@bp.route("/map/<datetime>/window/<window>", methods=("GET", "POST"))
def stopped_on_map(datetime, window):
    error = validate_date_input(datetime, window)
    if error is None:
        vehicles = query_for_vehicles(datetime, window, 25)
    return render_template("/location/map_stopped.html", vehicles=vehicles, googleAPIkey=GOOGLE_MAPS_KEY)


@bp.route("/stoppedTimeHistorical", methods=("GET", "POST"))
def stopped_by_time_hist():
    if request.method == "POST":
        hour_of_day = request.form["hour"]
        min_stop_length = request.form["minstop"]
        max_stop_length = request.form["maxstop"]

        (error, hour_int, min_int, max_int) = validate_time_input(hour_of_day, min_stop_length, max_stop_length)

        if error is not None:
            flash(error)
        else:
            vehicles = query_for_vehicles_hist(hour_int, 60*min_int, 60*max_int, 300)
            return render_template("location/time_stopped.html", vehicles=vehicles)

    return render_template("location/time_stopped.html")


@bp.route("/heatmap/<hour>/min/<min>/max/<max>", methods=("GET", "POST"))
def stopped_on_heatmap(hour, min, max):
    (error, hour_int, min_int, max_int) = validate_time_input(hour, min, max)
    if error is None:
        vehicles = query_for_vehicles_hist(hour_int, 60*min_int, 60*max_int, 3000)
    return render_template("/location/heatmap_stopped.html", vehicles=vehicles, googleAPIkey=GOOGLE_MAPS_KEY)
