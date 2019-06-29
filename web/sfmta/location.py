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
from sfmta.config import GOOGLE_MAPS_KEY

bp = Blueprint("location", __name__)
dt_regexp = "^201\d{1}-[01]\d-[012]\d [012]\d:[0-5]\d:[0-5]\d$"


def validate_date_input(datetime, time_window):
    error = None

    if not datetime:
        error = "Date and time are required."

    if not re.search(dt_regexp, datetime):
        error = "Date and time must be in the format YYYY-MM-DD hh:mm:ss and be between 2013-01-01 and 2015-12-28."

    if not time_window:
        error = "Time window is required."

    try:
        time_window_int = int(time_window)
        if not (1 <= time_window_int <= 120):
            error = "Time window must be between 1 and 120."
    except ValueError:
        error = 'Please enter an integer value for the time window.'

    return error


def query_for_vehicles(datetime, time_window, limit = 0):
    function_result_proxy = db_session.execute("select list_stopped_vehicles(:datetime, :window)",
        {"datetime": datetime, "window": time_window})
    vehicles = []
    db_vehicles = function_result_proxy.fetchall()
    vehicle_list = db_vehicles if limit == 0 else db_vehicles[:limit]
    for row in vehicle_list:
        d = dict(row.items())
        rawdata = d["list_stopped_vehicles"].split(",")
        vehicle = []
        vehicle.append(rawdata[0][1:])
        vehicle.append('' if rawdata[1] == "\"\"" else rawdata[1])
        vehicle.append(rawdata[2][1:20])
        vehicle.append(round(float(rawdata[3]) / 60, 1))
        vehicle.append(rawdata[4])
        vehicle.append(rawdata[5][:-1])
        vehicles.append(vehicle)
    function_result_proxy.close()
    return vehicles


@bp.route("/stoppedDate", methods=("GET", "POST"))
def stopped_by_date():
    if request.method == "POST":
        datetime = request.form["datetime"]
        time_window = request.form["timewindow"]
        error = validate_date_input(datetime, time_window)

        if error is not None:
            flash(error)
        else:
            vehicles = query_for_vehicles(datetime, time_window)
            return render_template("location/index.html", vehicles=vehicles)

    return render_template("location/index.html")


@bp.route("/map/<datetime>/window/<window>", methods=("GET", "POST"))
def stopped_on_map(datetime, window):
    error = validate_date_input(datetime, window)
    if error is None:
        vehicles = query_for_vehicles(datetime, window, 25)
    return render_template("/location/map_stopped.html", vehicles=vehicles, googleAPIkey=GOOGLE_MAPS_KEY)


def validate_time_input(hour_of_day, min_stop, max_stop):
    error = None

    if not hour_of_day:
        error = "Hour of day is required."

    try:
        hour_of_day_int = int(hour_of_day)
        if not (0 <= hour_of_day_int <= 23):
            error = "Hour of day must be between 0 and 23."
    except ValueError:
        error = 'Please enter an integer value for the hour of day.'

    try:
        min_stop_int = int(min_stop)
        if not (min_stop_int >= 0):
            error = "If entered, minimum stop length must be positive."
    except ValueError:
        error = 'Please enter an integer value for the minimum stop length.'

    try:
        max_stop_int = int(max_stop)
        if not (max_stop_int >= 0):
            error = "If entered, maximum stop length must be positive."
    except ValueError:
        error = 'Please enter an integer value for the maximum stop length.'

    return error, hour_of_day_int, min_stop_int, max_stop_int


def query_for_vehicles_hist(hour, min = 0, max = 0, limit = 0):
    query = """SELECT vehicle_tag, report_time, stopped_for, latitude, longitude
        FROM sfmta_stops_det_10
        WHERE hour_of_day = :hr """
    if min > 0 and max > 0:
        where = "AND stopped_for BETWEEN :min_stop AND :max_stop"
        query_parms = {"hr": hour, "min_stop": min, "max_stop": max}
    elif min > 0:
        where = "AND stopped_for > :min_stop"
        query_parms = {"hr": hour, "min_stop": min}
    elif max > 0:
        where = "AND stopped_for < :max_stop"
        query_parms = {"hr": hour, "max_stop": max}
    else:
        where = ""
        query_parms = {"hr": hour}
    function_result_proxy = db_session.execute(query + where, query_parms)
    vehicles = []
    db_vehicles = function_result_proxy.fetchall()
    vehicle_list = db_vehicles if limit == 0 else db_vehicles[:limit]
    for row in vehicle_list:
        d = dict(row.items())
        rawdata = d["list_stopped_vehicles"].split(",")
        vehicle = []
        vehicle.append(rawdata[0][1:])
        vehicle.append(rawdata[1][1:20])
        vehicle.append(round(float(rawdata[3]) / 60, 1))
        vehicle.append(rawdata[4])
        vehicle.append(rawdata[5][:-1])
        vehicles.append(vehicle)
    function_result_proxy.close()
    return vehicles


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
            vehicles = query_for_vehicles_hist(hour_int, min_int, max_int)
            return render_template("location/time_stopped.html", vehicles=vehicles)

    return render_template("location/time_stopped.html")


@bp.route("/heatmap/<hour>/min/<min>/max/<max>", methods=("GET", "POST"))
def stopped_on_heatmap(hour, min, max):
    (error, hour_int, min_int, max_int) = validate_time_input(hour, min, max)
    if error is None:
        vehicles = query_for_vehicles_hist(hour_int, min_int, max_int, 25)
    return render_template("/location/heatmap_stopped.html", vehicles=vehicles, googleAPIkey=GOOGLE_MAPS_KEY)
