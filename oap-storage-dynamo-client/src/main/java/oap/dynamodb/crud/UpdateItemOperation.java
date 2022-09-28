package oap.dynamodb.crud;

import oap.dynamodb.Key;
import oap.dynamodb.annotations.API;

import java.util.Map;

import static oap.dynamodb.crud.OperationType.UPDATE;

@API
public class UpdateItemOperation extends AbstractOperation {
    public UpdateItemOperation( String name ) {
        super( UPDATE, name );
    }

    public UpdateItemOperation( Key key, Map<String, Object> binNamesAndValues ) {
        super( UPDATE, key, binNamesAndValues );
    }
}
