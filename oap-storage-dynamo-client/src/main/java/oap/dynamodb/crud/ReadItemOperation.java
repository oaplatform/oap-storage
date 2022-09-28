package oap.dynamodb.crud;

import oap.dynamodb.Key;
import oap.dynamodb.annotations.API;

import java.util.Map;

@API
public class ReadItemOperation extends AbstractOperation {
    public ReadItemOperation( String name ) {
        super( OperationType.READ, name );
    }

    public ReadItemOperation( Key key, Map<String, Object> binNamesAndValues ) {
        super( OperationType.READ, key, binNamesAndValues );
    }
}
