from decimal import Decimal
import json
import boto3
import logging
from botocore.exceptions import ClientError
from pprint import pprint

logger = logging.getLogger(__name__)

class Forum:
    """Encapsulates an Amazon DynamoDB table of Forum data."""
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.

        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists
    
    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table that can be used to store forum data.
        The table partition key(S): Name 

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'Name', 'KeyType': 'HASH'},  # Partition key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'Name', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 5})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table
        

    def write_batch(self, forums):
        """
        Fills an Amazon DynamoDB table with the specified data, using the Boto3
        Table.batch_writer() function to put the items in the table.
        Inside the context manager, Table.batch_writer builds a list of
        requests. On exiting the context manager, Table.batch_writer starts sending
        batches of write requests to Amazon DynamoDB and automatically
        handles chunking, buffering, and retrying.

        :param forums: The data to put in the table. Each item must contain at least
                       the keys required by the schema that was specified when the
                       table was created.
        """
        try:
            with self.table.batch_writer() as writer:
                for forum in forums:
                    writer.put_item(Item=forum)
        except ClientError as err:
            logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    def get_sample_forum_data(self,forum_file_name):
        """
        Gets sample forum data, either from a local file.

        :param forum_file_name: The local file name where the forum data is stored in JSON format.
        :return: The forum data as a dict.
        """
        try:
            with open(forum_file_name) as forum_file:
                forum_data = json.load(forum_file, parse_float=Decimal)
        except FileNotFoundError:
            print(f"File {forum_file_name} not found. You must first download the file to "
                "run this demo...")
            raise
        else:
            # return .
            return forum_data
    def get_forum(self, name):
        """
        Gets forum data from the table for a specific forum.

        :param name: The name of the forum.
        :return: The data about the requested forum.
        """
        try:
            response = self.table.get_item(Key={'Name': name})
        except ClientError as err:
            logger.error(
                "Couldn't get forum %s from table %s. Here's why: %s: %s",
                name, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']
        

    def scan_forums(self):
        """
        Scans for forums.

        :param n/a
        :return: The list of forums.
        """
        forums = []
        scan_kwargs = {}
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs['ExclusiveStartKey'] = start_key
                response = self.table.scan(**scan_kwargs)
                forums.extend(response.get('Items', []))
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
        except ClientError as err:
            logger.error(
                "Couldn't scan for forums. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

        return forums

    def add_forum(self, name, category, messages, threads, views):
        """
        Adds a forum to the table.

        :param name: The name of the forum.
        :param category: The category of the forum.
        :param messages: The messages of the forum.
        :param threads: The quality threads of the forum.
        :param views: The quality views of the forum.
        """
        try:
            self.table.put_item(
                Item={
                    'Name': name,
                    'Category': category,
                    'Messages': messages,
                    'Threads': threads,
                    'Views': views
                    })
        except ClientError as err:
            logger.error(
                "Couldn't add forum %s to table %s. Here's why: %s: %s",
                name, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def update_forum(self, name, category, messages, threads, views):
        """
        Updates rating and plot data for a forum in the table.

        :param name: The name of the forum.
        :param category: The category of the forum.
        :param messages: The messages of the forum.
        :param threads: The quality threads of the forum.
        :param views: The quality views of the forum.
        :return: The fields that were updated, with their new values.
        """
        try:
            response = self.table.update_item(
                Key={'Name': name},
                UpdateExpression="set Category=:c, Messages=:m, Threads=:t, #Views=:v",
                ExpressionAttributeValues={
                    ':c': category,
                    ':m': messages,
                    ':t': threads,
                    ':v': views
                    },
                ExpressionAttributeNames={"#Views" : "Views"},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update forum %s in table %s. Here's why: %s: %s",
                name, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']
    
    def delete_forum(self, name):
        """
        Deletes a forum from the table.

        :param name: The title of the forum to delete.
        """
        try:
            self.table.delete_item(Key={'Name': name})
        except ClientError as err:
            logger.error(
                "Couldn't delete forum %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def delete_table(self):
        """
        Deletes the table.
        """
        try:
            self.table.delete()
            self.table = None
        except ClientError as err:
            logger.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

if __name__ == '__main__':
 
    dynamodb = boto3.resource('dynamodb')
    table_name = 'Forum'
    forum_file_name = './sampledata/Forum.json'

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    print('-'*88)
    print("Welcome to the Amazon DynamoDB getting started demo.")
    print('-'*88)

    forums = Forum(dynamodb)
    #Check for table existence, create table if not found
    forums_exists = forums.exists(table_name)
    if not forums_exists:
        print(f"\nCreating table {table_name}...")
        forums.create_table(table_name)
        print(f"\nCreated table {forums.table.name}.")

    #Load data into the created table
    forum_data = forums.get_sample_forum_data(forum_file_name)
    print(f"\nReading data from '{forum_file_name}' into your table.")
    forums.write_batch(forum_data)
    print(f"\nWrote {len(forum_data)} forums into {forums.table.name}.")
    print('-'*88)
    
    #Get forum data with hash key = 'Amazon DynamoDB'  
    forum = forums.get_forum("Amazon DynamoDB")
    print("\nHere's what I found:")
    pprint(forum)
    print('-'*88)

    #Add new forum data with hash key = 'SQL server'
    forums.add_forum("SQL server","Amazon Web Services",4,2,1000)
    print(f"\nAdded item to '{forums.table.name}'.")
    print('-'*88)

    #Full scan table
    releases = forums.scan_forums()
    if releases:
        print(f"\nHere are your {len(releases)} forums:\n")
        pprint(releases)
    else:
        print(f"I don't know about any forums released\n")
    print('-'*88)

    #Update data: update forum quality views from 1000 to 2000
    updated = forums.update_forum("SQL server","Amazon Web Services",4,2,2000)
    print(f"\nUpdated :")
    pprint(updated)
    print('-'*88)

    #Delete data
    forums.delete_forum("SQL server")
    print(f"\nRemoved item from the table.")
    print('-'*88)

    ##Full scan table
    releases = forums.scan_forums()
    if releases:
        print(f"\nHere are your {len(releases)} forums:\n")
        pprint(releases)
    else:
        print(f"I don't know about any forums released\n")
    print('-'*88)

    #List all table
    print('-'*88)
    print(f"Table list:\n")
    print(list(dynamodb.tables.all()))
    
    #Delete table
    forums.delete_table()
    print(f"Deleted {table_name}.")

    print('-'*88)
    print("Don't forget to delete the table when you're done or you might incur "
              "charges on your account.")
    print("\nThank you for visiting Devopsroles.com !")
    print('-'*88)