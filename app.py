import time

from flask import Flask, jsonify, request, abort

from db import (
    get_items,
    get_item,
    delete_item,
    update_item,
    put_item,
)

app = Flask(__name__)

# print(__name__)

data = [{"id": i, "name": f"Restaurant {i}"} for i in range(1, 1001)]

PAGE_LIMIT = 50


def get_page(data, url, previous, next):
    page = {}

    # page["limit"] = PAGE_LIMIT
    total = len(data)
    page["total"] = total

    # if total < start or limit < 0:
    #     abort(404)

    if not previous:
        page["previous"] = ""
    else:
        page["previous"] = url + f"&previous={previous}"

    if not next:
        page["next"] = ""
    else:
        page["next"] = url + f"&next={next}"

    page["restaurants"] = data

    return page


@app.route("/")
def welcome_message():
    return "Welcome to Restaurants App"


@app.route("/api/restaurants", methods=["GET"])
def get_restaurants():
    try:
        next = request.args.get("next", "")

        previous = request.args.get("previous", "")

        city = request.args.get("city", "")

        limit = int(request.args.get("limit", PAGE_LIMIT))

        print("Next of", next)

        print("Previous of", previous)

        print("City", city)

        print("Limit", limit)

        if previous:
            result = get_items(
                limit=limit, start_key=previous, backward=True, city=city
            )
            restaurants = result["items"]
            first_item = result["last_evaluated_key"]
            last_item = restaurants[0]["city"]
            restaurants.reverse()
            # print("Reverse", restaurants)
        else:
            result = get_items(limit=limit, start_key=next, city=city)
            restaurants = result["items"]
            last_item = result["last_evaluated_key"]
            if next or previous:
                first_item = restaurants[0]["city"]
            else:
                first_item = None

        if first_item:
            first_item = first_item.replace("CITY###", "").replace("###", "-")

        if last_item:
            last_item = last_item.replace("CITY###", "").replace("###", "-")

        # restaurants = data

        # if restaurants:
        #     return jsonify(restaurants), 200
        # else:
        #     return jsonify({"message": "Restaurants not found"}), 404

        response = get_page(
            restaurants,
            "/api/restaurants" + f"?limit={limit}" + (f"&city={city}" if city else ""),
            previous=first_item,
            next=last_item,
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
        data["id"] = str(restaurant_id)
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
        data["id"] = str(restaurant_id)
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
