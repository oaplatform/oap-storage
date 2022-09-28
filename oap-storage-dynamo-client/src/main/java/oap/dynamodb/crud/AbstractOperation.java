package oap.dynamodb.crud;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import oap.dynamodb.DynamodbClient;
import oap.dynamodb.Key;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;

import java.util.Collections;
import java.util.Map;

@Getter
@ToString( exclude = { "putRequest", "deleteRequest" } )
public abstract class AbstractOperation {
    private final OperationType type;
    @Setter
    private DynamodbClient.State state;
    private String name;
    private final Key key;
    private final Map<String, Object> binNamesAndValues;

    @Setter
    private PutRequest putRequest;
    @Setter
    private DeleteRequest deleteRequest;


    protected AbstractOperation( OperationType type, String name ) {
        this.type = type;
        this.name = name;
        key = null;
        binNamesAndValues = Collections.emptyMap();
    }

    protected AbstractOperation( OperationType type, Key key, Map<String, Object> binNamesAndValues ) {
        this.type = type;
        this.key = key;
        this.binNamesAndValues = binNamesAndValues;
    }
}
