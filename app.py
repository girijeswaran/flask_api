import time

from flask import Flask, jsonify, request, abort

from db import read_table, read_items, get_item, delete_item, update_item, put_item

app = Flask(__name__)

# print(__name__)

data = [{"id": i, "name": f"Restaurant {i}"} for i in range(1, 1001)]

PAGE_LIMIT = 5


def get_page(data, url, start, limit):
    page = {}

    start = int(start)
    limit = int(limit)
    page["start"] = start
    page["limit"] = limit
    total = len(data)
    page["total"] = total

    if total < start or limit < 0:
        abort(404)
    if start == 1:
        page["previous"] = ""
    else:
        previous_start = max(start - limit, 1)
        page["previous"] = url + f"?start={previous_start}&limit={limit}"

    if start + limit > total:
        page["next"] = ""
    else:
        next_start = start + limit
        page["next"] = url + f"?start={next_start}&limit={limit}"

    page["restaurants"] = data[start - 1 : (start - 1) + limit]

    return page


@app.route("/")
def welcome_message():
    return "Welcome to Restaurants App"


@app.route("/api/restaurants", methods=["GET"])
def get_restaurants():
    try:
        # restaurants = read_items()

        restaurants = data

        # # time.sleep(10)

        # if restaurants:
        #     return jsonify(restaurants), 200
        # else:
        #     return jsonify({"message": "Restaurants not found"}), 404

        response = get_page(
            restaurants,
            "/api/restaurants",
            start=request.args.get("start", 1),
            limit=request.args.get("limit", 5),
        )

        return response

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["GET"])
def get_specific_restaurant(restaurant_id):
    try:
        restaurant = get_item(restaurant_id)

        # time.sleep(10)

        if restaurant:
            return jsonify(restaurant)
        else:
            return jsonify({"message": f"Restaurant {restaurant_id} not found"}), 404

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["POST"])
def create_restaurant(restaurant_id):
    try:
        data = request.json
        data["id"] = restaurant_id
        # print(data)
        response = put_item(data)

        if response:
            return jsonify({"message": f"Restaurant {restaurant_id} Created"}), 201
        else:
            return (
                jsonify({"message": f"Issue in creating restaurant {restaurant_id}"}),
                404,
            )

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["PUT"])
def update_restaurant(restaurant_id):
    try:
        data = request.json
        data["id"] = restaurant_id
        # print(data)
        response = update_item(data)

        if response:
            return jsonify({"message": f"Restaurant {restaurant_id} updated"}), 200
        else:
            return jsonify({"message": f"Restaurant {restaurant_id} not found"}), 404

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["DELETE"])
def delete_restaurant(restaurant_id):
    try:
        response = delete_item(restaurant_id)

        print(response)

        if response:
            return "", 204
        else:
            return jsonify({"message": f"Restaurant {restaurant_id} not found"}), 404

    except Exception as e:
        print(e)


@app.errorhandler(405)
def method_not_allowed(error):
    return "Method Not Allowed", 405


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
