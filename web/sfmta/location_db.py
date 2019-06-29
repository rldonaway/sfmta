from sfmta.db import db_session
from sfmta.models import Vehicle, VehicleStop


def query_for_vehicles(datetime, time_window, limit=0):
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


def query_for_vehicles_hist(hour, min_stop=0, max_stop=0, limit=0):
    vehicles_by_hour = db_session.query(VehicleStop).filter(VehicleStop.hour_of_day == hour)

    if min_stop > 0 and max_stop > 0:
        vehicles_qry = vehicles_by_hour\
            .filter(VehicleStop.stopped_for >= min_stop)\
            .filter(VehicleStop.stopped_for <= max_stop)
    elif min_stop > 0:
        vehicles_qry = vehicles_by_hour.filter(VehicleStop.stopped_for >= min_stop)
    elif max_stop > 0:
        vehicles_qry = vehicles_by_hour.filter(VehicleStop.stopped_for <= max_stop)
    else:
        vehicles_qry = vehicles_by_hour

    if limit == 0:
        return vehicles_qry.all()
    else:
        return vehicles_qry.limit(limit)
