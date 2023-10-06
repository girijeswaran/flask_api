import sys

from db import (
    get_item,
    delete_item,
    update_item,
    put_item,
)

sys.path.append("..")

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


def test_get_item():
    response = get_item("1")

    if response:
        assert True
    else:
        assert False


def test_put_item():
    response = put_item(data)

    if response:
        assert True
    else:
        assert False


def test_delete_item():
    response = delete_item("4")

    if response:
        assert True
    else:
        assert False
