import requests
from flask import request, jsonify, Blueprint

from api.models.constants import ETL
from api.utils.misc import validate_json, require_auth

etl = Blueprint("etl", __name__)


@etl.route("/etl/saturn-tables", methods=["GET"])
@validate_json
@require_auth
def get_saturn_tables():
    res = requests.get(f"{ETL}/saturn_tables").json()
    return jsonify(response=res)


@etl.route("/etl/saturn-tables", methods=["POST"])
@validate_json
@require_auth
def post_saturn_tables():
    columns = [
        "table_id",
        "dataset_id",
        "period",
        "request_type",
        "is_use",
        "sla_hours",
        "retention_days",
        "description",
        "target_systems",
    ]
    keys = request.json.keys()
    data = {}

    def if_empty_default(column, compare, default, keys=keys, data=data):
        if column == compare and column not in keys:
            data[column] = default

    for col in columns:
        data[col] = request.json.get(col, "")
        if_empty_default(col, "is_use", True)
        if_empty_default(col, "sla_hours", 31)
        if_empty_default(col, "retention_days", 180)
        if_empty_default(col, "period", "daily")

    res = requests.post(f"{ETL}/saturn_tables", json=data)

    if "error" in res.text:
        print(f'[post_saturn_tables] {res.json()["error"]}')
        return jsonify(response=res.json()["error"]), res.status_code
    else:
        return jsonify(response=res.text)
