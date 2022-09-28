package oap.dynamodb.crud;

import oap.dynamodb.Key;
import oap.dynamodb.annotations.API;

import java.util.Map;

import static oap.dynamodb.crud.OperationType.CREATE;

@API
public class CreateItemOperation extends AbstractOperation {
    public CreateItemOperation( String name ) {
        super( CREATE, name );
    }

    public CreateItemOperation( Key key, Map<String, Object> binNamesAndValues ) {
        super( CREATE, key, binNamesAndValues );
    }
}
