import json
import time
import boto3
from decimal import Decimal
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

dynamodb = boto3.client(
    "dynamodb",
    region_name="eu-north-1",  # , endpoint_url="https://dynamodb.eu-north-1.amazonaws.com/"
)

table_name = "restaurants_final"

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

        key_schema = [
            {"AttributeName": "id", "KeyType": "HASH"},
            {
                "AttributeName": "city",
                "KeyType": "RANGE",  # RANGE corresponds to the sort key
            },
        ]

        attribute_definitions = [
            {"AttributeName": "id", "AttributeType": "S"},
            {
                "AttributeName": "city",
                "AttributeType": "S",  # N corresponds to Number data type
            },
        ]

        provisioned_throughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}

        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=provisioned_throughput,
        )

        # Wait for the table to be created
        table_waiter = dynamodb.get_waiter("table_exists")
        table_waiter.wait(TableName=table_name)

        print("Table Created Successfully")

    except dynamodb.exceptions.ResourceInUseException:
        print("Table already exists")

    except Exception as e:
        print(f"An error occurred: {e}")


def put_item(item_dict):
    try:
        if item_dict.get("id") and item_dict.get("city"):
            serialized_item = {
                key: serializer.serialize(convert_float_to_decimal(value))
                for key, value in item_dict.items()
            }

            item_dict2 = item_dict.copy()

            item_dict2["id"] = "RESTAURANTS"

            item_dict2["city"] = "CITY###" + item_dict["city"] + "###" + item_dict["id"]

            serialized_item2 = {
                key: serializer.serialize(convert_float_to_decimal(value))
                for key, value in item_dict2.items()
            }

            # print(item_dict)
            # print("---------")
            # print(serialized_item)

            response = dynamodb.put_item(TableName=table_name, Item=serialized_item)

            response2 = dynamodb.put_item(TableName=table_name, Item=serialized_item2)

            # print(response)

            status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            status_code2 = response2["ResponseMetadata"]["HTTPStatusCode"]

            if status_code == 200 and status_code2 == 200:
                print("Put item operation was successful")
                return True
            else:
                print(f"Put item operation failed with an {status_code} status code.")

        else:
            print("Either id or city is missing!")

    except Exception as e:
        print(f"An error occurred: {e}")


def put_items(items_list):
    start = 9870 // 2

    items_list = items_list[start:]
    for item in items_list:
        put_item(item)


def get_item(id):
    try:
        # key = {"id": {"S": str(id)}}

        # response = dynamodb.get_item(TableName=table_name, Key=key)

        response = dynamodb.query(
            TableName=table_name,
            KeyConditionExpression="#id =:value",
            ExpressionAttributeValues={":value": {"S": str(id)}},
            ExpressionAttributeNames={"#id": "id"},
        )

        items = response.get("Items")

        if items:
            item = items[0]

            # print(type(item))

            item_dict = deserialize(item)

            print("Item Found:", item_dict)

            return item_dict

        else:
            print("Item not found.")

    except Exception as e:
        print(f"An error occurred: {e}")


def get_items(limit, start_key=None, backward=False, city=None):
    try:
        query_params = {}

        query_params["Limit"] = limit

        if start_key:
            # {"id": {"S": "RESTAURANTS"}, "city": {"S": "CITY###Abohar###156588"}}

            query_params["ExclusiveStartKey"] = {
                "id": {"S": "RESTAURANTS"},
                "city": {"S": "CITY###" + start_key.replace("-", "###")},
            }

        results = []
        last_range_key = None

        while True:
            print("Limit", query_params["Limit"])

            response = dynamodb.query(
                TableName=table_name,
                KeyConditionExpression="#id =:id AND begins_with (#city, :city )",
                ExpressionAttributeValues={
                    ":id": {"S": "RESTAURANTS"},
                    ":city": {"S": "CITY###" + (city if city else "")},
                },
                ExpressionAttributeNames={"#id": "id", "#city": "city"},
                ScanIndexForward=not backward,
                **query_params,
            )

            # print("Query Response", response)

            items = response.get("Items") or []

            items_list = []

            print("Items Length", len(items))

            i = 1
            for item in items:
                item_dict = deserialize(item)
                items_list.append(item_dict)
                # print(i, item_dict)
                i = i + 1

            results.extend(items_list)

            last_evaluated_key = response.get("LastEvaluatedKey")
            count = response.get("Count")

            print("Last Evaluated Key", last_evaluated_key)
            print("Scanned Count", response.get("ScannedCount"))
            print("Read Count", count)

            if last_evaluated_key:
                query_params["ExclusiveStartKey"] = last_evaluated_key

                query_params["Limit"] = query_params["Limit"] - count

                last_range_key = last_evaluated_key["city"]["S"]

            else:
                last_range_key = None

            print("Length of Results", len(results), "/", limit)

            if len(results) >= limit or not last_evaluated_key:
                break

        # print("All Items", results)

        return {"items": results, "last_evaluated_key": last_range_key}

    except Exception as e:
        print(e)


def update_item(item_dict):
    try:
        # Not Completed Yet

        # id = item_dict["id"]

        id = item_dict.pop("id", None)

        # only use this city for an update if id and existing_item_city = user_given_city

        city = item_dict.pop("city", None)

        item = get_item(id)

        if item:
            print("Restaurant Found!")
            city = item["city"]
        else:
            print(f"The given Restaurant with id = {id} not found!")
            return

        key = {"id": {"S": str(id)}, "city": {"S": city}}

        key2 = {"id": {"S": "RESTAURANTS"}, "city": {"S": f"CITY###{city}###{id}"}}

        keys = [key, key2]

        update_expression = "SET "

        expression_attribute_name = {}
        expression_attribute_value = {}

        # put item vs update item

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
        print(keys)

        status_codes = []

        for k in keys:
            response = dynamodb.update_item(
                TableName=table_name,
                Key=k,
                UpdateExpression=update_expression,
                # To Avoid Error like Attribute name is a reserved keyword; reserved keyword: name
                ExpressionAttributeNames=expression_attribute_name,
                ExpressionAttributeValues=expression_attribute_value,
                # ReturnValues="ALL_NEW",
            )

            status_codes.append(response["ResponseMetadata"]["HTTPStatusCode"])

            # print(response)

        # UpdateExpression = "SET #name = :name, #age = :age"
        # ExpressionAttributeNames = {"#name": "name", "#age": "age"}
        # ExpressionAttributeValues = {
        #     ":name": {"S": "Person A"},
        #     ":age": {"S": "11"},
        # }

        if status_codes[0] == 200 and status_codes[1] == 200:
            print("Update item operation was successful")
            return True
        else:
            print(f"Update item operation failed with an {status_codes} status code.")

    except Exception as e:
        print(f"An error occurred: {e}")


def delete_item(id):
    try:
        item = get_item(id)

        if item:
            print("Restaurant Found!")
            city = item["city"]
        else:
            print(f"The given Restaurant with id = {id} not found!")
            return

        key = {"id": {"S": str(id)}, "city": {"S": city}}

        key2 = {"id": {"S": "RESTAURANTS"}, "city": {"S": f"CITY###{city}###{id}"}}

        response = dynamodb.delete_item(TableName=table_name, Key=key)

        response2 = dynamodb.delete_item(TableName=table_name, Key=key2)

        # print(response)

        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        status_code2 = response2["ResponseMetadata"]["HTTPStatusCode"]

        if status_code == 200 and status_code2 == 200:
            print("delete item operation was successful")
            return True
        else:
            print(f"delete item operation failed with an {status_code} status code.")

    except Exception as e:
        print(f"An error occurred: {e}")


def insert_items():
    restaurants = read_json("final_dataset.json")

    # items = items["restaurants"] if items.get("restaurants") else items

    if isinstance(restaurants, list):
        print("Yes, List")
        put_items(restaurants)
    else:
        put_item(restaurants)


if __name__ == "__main__":
    if is_table(table_name) == False:
        create_table(table_name)
        insert_items()

    else:
        print("The Table Already Created!")

    insert_items()

    get_item("158203")

    # get_item("RESTAURANTS")

    item_dict = {
        "id": "158203",
        "city": "City Test",
        "name": "Update Test",
        "address": "Address Test",
    }

    # update_item(item_dict)

    # get_items(start_key="CITY###Abohar###531342")

    # delete_item("531342")
