from flask import Blueprint
from flask import render_template
from sfmta.models import Vehicle

bp = Blueprint("reference", __name__)


@bp.route("/vehicles")
def index():
    """Show all the posts, most recent first."""
    vehicles = Vehicle.query.order_by(Vehicle.vehicle_tag)
    return render_template("reference/index.html", vehicles=vehicles)


# @bp.route("/<int:id>/delete", methods=("POST",))
# @login_required
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
