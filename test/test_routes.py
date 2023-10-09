import pytest

import sys

sys.path.append("..")

from app import app

data = {
    "id": "4",
    "address": "Nawab Biryani, H.No.18-2-272, Revenue Ward No  18, Ashok Nagar, Tirupati, , Tirupati  (Urban), Chittoor, , Andhra Pradesh,  517501",
    "city": "Tirupati",
    "cost": "₹ 250",
    "cuisine": "Biryani,Kebabs",
    "lic_no": "license",
    "link": "https://www.swiggy.com/restaurants/nawab-biryani-korlagunta-tirupati-556095",
    "name": "Nawab Biryani Post Test",
    "rating": "--",
    "rating_count": "Too Few Ratings",
}


@pytest.fixture
def client():
    return app.test_client()


def test_get_restaurants(client):
    response = client.get("/api/restaurants")
    assert response.status_code == 200


def test_get_restaurant(client):
    response = client.get("/api/restaurants/156542")
    assert response.status_code == 200


def test_post_restaurant(client):
    response = client.post("/api/restaurants/4", json=data)
    assert response.status_code == 201


def test_put_restaurant(client):
    response = client.put("/api/restaurants/4", json=data)
    assert response.status_code == 200


def test_delete_restaurant(client):
    response = client.delete("/api/restaurants/4")
    assert response.status_code == 204


def test_405(client):
    response = client.delete("/api/restaurants")
    assert response.status_code == 405


if __name__ == "__main__":
    pytest.main()
