package oap.dynamodb.crud;

import oap.dynamodb.Key;
import oap.dynamodb.annotations.API;

import static oap.dynamodb.crud.OperationType.DELETE;

@API
public class DeleteItemOperation extends AbstractOperation {
    public DeleteItemOperation( String name ) {
        super( DELETE, name );
    }

    public DeleteItemOperation( Key key ) {
        super( DELETE, key, null );
    }
}
