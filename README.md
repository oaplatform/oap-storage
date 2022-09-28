# oap-storage

# MongoDB

Project uses in memory implementation of MongoDB for tests. 
In doesn't support the latest mongo wire protocol. Therefor the only mongo shell 
(required for mingrations) that can communicate to it has maximum version of 3.6.x.

# DynamoDB

Thin (simple) DynamoDB client. Used for sync/async operations in [DynamoDB].
Two services are configured based on DynamoDB client: _dynamodb-client-reader_ and _dynamodb-client-writer_ in

[oap-module.conf](oap-dynamo-db/oap-dynamodb/src/main/resources/META-INF/oap-module.conf)

DynamoDB logging is created by [DynamodbLog](oap-dynamo-db/oap-dynamodb/src/main/java/oap/dynamodb/DynamodbLog.java)
and configured by _dynamodb-log_ service in [oap-module.conf](oap-dynamo-db/oap-dynamodb/src/main/resources/META-INF/oap-module.conf).

Use [DynamodbFixture](oap-dynamo-db/oap-dynamodb/src/main/java/oap/dynamodb/DynamodbFixture.java) for any tests based on the DynamoDB
server. See [DynamodbClientTest](oap-dynamo-db/oap-dynamodb/src/test/java/oap/dynamodb/DynamodbClientTest.java) for any specific test cases.

##docker
docker pull amazon/dynamodb-local
docker run -p 8000:8000 amazon/dynamodb-local

##dynamodb.yaml
version: '2'
services:
dynamodb:
image: amazon/dynamodb-local:latest
ports:
- "8000:8000"
  command: ["-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"]

Note: There are plenty of ignored dynamo-db tests. All of them are based on DynamoDB testcontainer.