from flask import Blueprint, render_template

__all__ = ["search_blueprint"]

search_blueprint = Blueprint("search", __name__, url_prefix="/search")

@search_blueprint.route("/")
def search():
    # Render main page template
    return render_template("search/search_page.html")