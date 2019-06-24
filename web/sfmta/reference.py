from flask import Blueprint
from flask import render_template
from sfmta.models import Vehicle

bp = Blueprint("reference", __name__)


@bp.route("/vehicles")
def show_vehicles():
    vehicles = Vehicle.query.order_by(Vehicle.vehicle_tag)
    return render_template("reference/index.html", vehicles=vehicles)
