import json
import boto3
from decimal import Decimal
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

dynamodb = boto3.client(
    "dynamodb", region_name="us-east-1", endpoint_url="http://localhost:8000"
)

table_name = "restaurants"

serializer = TypeSerializer()

deserializer = TypeDeserializer()


def read_json(filename):
    try:
        with open(filename, "r", encoding="utf-8") as file:
            return json.load(file)

    except Exception as e:
        print(e)


def convert_float_to_decimal(value):
    if isinstance(value, float):
        c_value = Decimal(str(value))
    elif isinstance(value, dict):
        c_value = {k: convert_float_to_decimal(v) for k, v in value.items()}
    elif isinstance(value, list):
        c_value = [convert_float_to_decimal(d) for d in value]
    else:
        c_value = value

    return c_value


def deserialize(item):
    return {k: deserializer.deserialize(v) for k, v in item.items()}


def list_tables():
    try:
        response = dynamodb.list_tables()

        print(response)

        if "TableNames" in response:
            print("DynamoDB Local is running.")
        else:
            print("DynamoDB Local is not running or responsive.")

    except Exception as e:
        print(f"An error occurred: {e}")


def is_table(table_name):
    try:
        response = dynamodb.describe_table(TableName=table_name)
        print(f"Connected to the table '{table_name}' successfully.")
        return True

    except dynamodb.exceptions.ResourceNotFoundException:
        print(f"The table '{table_name}' does not exist.")
        return False

    except Exception as e:
        print(f"An error occurred: {e}")


def create_table(table_name):
    try:
        # table_name = "restaurants"

        key_schema = [{"AttributeName": "id", "KeyType": "HASH"}]

        attribute_definitions = [{"AttributeName": "id", "AttributeType": "N"}]

        provisioned_throughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}

        dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=provisioned_throughput,
        )

        print("Table Created Successfully")

    except dynamodb.exceptions.ResourceInUseException:
        print("Table already exists")

    except Exception as e:
        print(f"An error occurred: {e}")


def read_table():
    try:
        response = dynamodb.scan(TableName=table_name)

        items = response.get("Items", [])

        items_dict = []

        for item in items:
            item_dict = deserialize(item)
            items_dict.append(item_dict)
            print(item_dict)

        print("No of Items", len(items_dict))

        return items

    except Exception as e:
        print(f"An error occurred: {e}")


def put_item(item_dict):
    try:
        serialized_item = {
            key: serializer.serialize(convert_float_to_decimal(value))
            for key, value in item_dict.items()
        }

        # print(item_dict)
        # print("---------")
        # print(serialized_item)

        response = dynamodb.put_item(TableName=table_name, Item=serialized_item)

        # print(response)

        status_code = response["ResponseMetadata"]["HTTPStatusCode"]

        if status_code == 200:
            print("Put item operation was successful")
            return True
        else:
            print(f"Put item operation failed with an {status_code} status code.")

    except Exception as e:
        print(f"An error occurred: {e}")


def put_items(items_list):
    for item in items_list:
        put_item(item)


def get_item(id):
    try:
        key = {"id": {"N": str(id)}}

        response = dynamodb.get_item(TableName=table_name, Key=key)

        item = response.get("Item")

        if item:
            # print(type(item))

            item_dict = deserialize(item)

            print(item_dict)

            return item_dict

        else:
            print("Item not found.")

    except Exception as e:
        print(f"An error occurred: {e}")


def update_item(item_dict):
    try:
        # Not Completed Yet

        # id = item_dict["id"]

        id = item_dict.pop("id", None)

        key = {"id": {"N": str(id)}}

        update_expression = "SET "

        expression_attribute_name = {}
        expression_attribute_value = {}

        # serialized_item = {
        #     key: serializer.serialize(convert_float_to_decimal(value))
        #     for key, value in item_dict.items()
        # }

        for k, v in item_dict.items():
            update_expression += f"#{k} = :{k}, "
            expression_attribute_name[f"#{k}"] = k
            expression_attribute_value[f":{k}"] = serializer.serialize(
                convert_float_to_decimal(v)
            )

        print(update_expression)
        # Remove the trailing comma and space
        update_expression = update_expression[:-2]

        print(update_expression)
        print(expression_attribute_value)
        print(key)

        response = dynamodb.update_item(
            TableName=table_name,
            Key=key,
            UpdateExpression=update_expression,
            # To Avoid Error like Attribute name is a reserved keyword; reserved keyword: name
            ExpressionAttributeNames=expression_attribute_name,
            ExpressionAttributeValues=expression_attribute_value,
            # ReturnValues="ALL_NEW",
        )

        # print(response)

        # UpdateExpression = "SET #name = :name, #age = :age"
        # ExpressionAttributeNames = {"#name": "name", "#age": "age"}
        # ExpressionAttributeValues = {
        #     ":name": {"S": "Person A"},
        #     ":age": {"S": "11"},
        # }

        status_code = response["ResponseMetadata"]["HTTPStatusCode"]

        if status_code == 200:
            print("Update item operation was successful")
            return True
        else:
            print(f"Update item operation failed with an {status_code} status code.")

    except Exception as e:
        print(f"An error occurred: {e}")


def delete_item(id):
    try:
        key = {"id": {"N": str(id)}}

        response = dynamodb.delete_item(TableName=table_name, Key=key)

        print(response)

        status_code = response["ResponseMetadata"]["HTTPStatusCode"]

        if status_code == 200:
            print("delete item operation was successful")
            return True
        else:
            print(f"delete item operation failed with an {status_code} status code.")

    except Exception as e:
        print(f"An error occurred: {e}")


# create_table(table_name)

if __name__ == "__main__":
    if is_table(table_name) == False:
        create_table(table_name)

        items = read_json("restaurants.json")

        items = items["restaurants"] if items.get("restaurants") else items

        if isinstance(items, list):
            print("Yes, List")
            put_items(items)
        else:
            put_item(items)

    else:
        print("The Table Already Created!")

    read_table()

    # get_item(id=1)

    item_dict = {
        "id": 1,
        "name": "Mission Chinese Food",
        "neighborhood": "Manhattan",
        "photograph": "Photo_1.jpg",
        "address": "171 E Broadway, New York, NY 10002",
        "latlng": {"lat": 40.713829, "lng": -73.989667},
    }

    # delete_item(id=11)

    # update_item(item_dict)

    # get_item(id=1)
