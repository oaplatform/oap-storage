package oap.dynamodb.crud;

import oap.dynamodb.annotations.API;

@API
public enum OperationType {
    CREATE,
    UPDATE,
    DELETE,
    READ
}
