from flask import Flask, jsonify, request

from db import read_table, get_item, delete_item, update_item, put_item

app = Flask(__name__)

# print(__name__)


@app.route("/")
def welcome_message():
    return "Welcome to Restaurants App"


@app.route("/api/restaurants", methods=["GET"])
def get_restaurants():
    try:
        restaurants = read_table()

        if restaurants:
            return jsonify(restaurants)
        else:
            return jsonify({"message": "Restaurants not found"}), 404

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["GET"])
def get_specific_restaurant(restaurant_id):
    try:
        restaurant = get_item(restaurant_id)

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
            return jsonify({"message": f"Restaurant {restaurant_id} Created"})
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
            return jsonify({"message": f"Restaurant {restaurant_id} updated"})
        else:
            return jsonify({"message": f"Restaurant {restaurant_id} not found"}), 404

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["DELETE"])
def delete_restaurant(restaurant_id):
    try:
        response = delete_item(restaurant_id)

        if response:
            return jsonify({"message": f"Restaurant {restaurant_id} deleted"})
        else:
            return jsonify({"message": f"Restaurant {restaurant_id} not found"}), 404

    except Exception as e:
        print(e)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
