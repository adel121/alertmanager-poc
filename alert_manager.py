import uuid
import boto3
from boto3.dynamodb.conditions import Key

class AlertManager():
    def __init__(self, table_name: str):
        self.dbclient = boto3.client('dynamodb', region_name="eu-central-1")
        self.dynamodb = boto3.resource('dynamodb', region_name="eu-central-1")
        self.table_name = table_name
        self.table = self.dynamodb.Table(table_name)

    # Helper method to create the composite partition key
    def _create_partition_key(self, city: str, category: str) -> str:
        return f"{city}:{category}"

    # Method to register an alert
    def register_alert(self, cities: list[str], categories: list[str], phone_number: str, name: str):
        uid = uuid.uuid4().hex
        for city in cities:
            for category in categories:
                partition_key = self._create_partition_key(city, category)
                self.dbclient.put_item(
                    TableName=self.table_name,
                    Item={
                        'CityAndCategory': {
                            'S': partition_key
                        },
                        'AlertID': {
                            'S': uid,
                        },
                        'PhoneNumber': {
                            'S': phone_number,
                        },
                        'Name': {
                            'S': name,
                        },
                    },
                )
        return uid

    # Method to deregister an alert by its AlertID
    def deregister_alert(self, alert_uid: str):
        # Scan and delete all entries matching the given alert_uid
        response = self.dbclient.scan(
            TableName=self.table_name,
            FilterExpression='AlertID = :alert_uid',
            ExpressionAttributeValues={
                ':alert_uid': {
                    'S': alert_uid
                }
            }
        )

        for item in response.get('Items', []):
            partition_key = item['CityAndCategory']['S']
            self.dbclient.delete_item(
                TableName=self.table_name,
                Key={
                    'CityAndCategory': {
                        'S': partition_key,
                    },
                    'AlertID': {
                        'S': alert_uid,
                    },
                },
            )

    # Method to match requests based on city, category, and other attributes
    def match_request(self, city: str, category: str, phone_number: str, description: str):
        partition_key = self._create_partition_key(city, category)

        # Query the DynamoDB table for matching alerts based on city and category
        response = self.table.query(
            KeyConditionExpression="CityAndCategory = :partition_key",
            ExpressionAttributeValues={
                ':partition_key': partition_key
            }
        )

        # Filter the results by phone_number if needed
        matching_alerts = [
            [ item.get("AlertID", None), item.get("PhoneNumber", None), item.get("Name",None)] for item in response.get('Items', [])
        ]

        return matching_alerts




