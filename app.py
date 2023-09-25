from flask import Flask, jsonify, request

from db import read_table, get_item, delete_item

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

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["GET"])
def get_specific_restaurant(restaurant_id):
    try:
        restaurant = get_item(restaurant_id)

        if restaurant:
            return jsonify(restaurant)
        else:
            return jsonify({"message": "Item not found"}), 404

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["PUT"])
def update_restaurant(item_id):
    try:
        data = request.json
        print(data)

    except Exception as e:
        print(e)


@app.route("/api/restaurants/<int:restaurant_id>", methods=["DELETE"])
def delete_restaurant(restaurant_id):
    try:
        response = delete_item(restaurant_id)

        if response:
            return jsonify({"message": "Item deleted"})
        else:
            return jsonify({"message": "Item not found"}), 404

    except Exception as e:
        print(e)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
