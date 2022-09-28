package oap.dynamodb.batch;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Setter;
import oap.dynamodb.DynamodbClient;
import oap.dynamodb.crud.AbstractOperation;
import oap.dynamodb.crud.DynamoDbHelper;
import oap.dynamodb.crud.OperationType;
import oap.dynamodb.modifiers.KeysAndAttributesModifier;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadBatchOperationHelper extends DynamoDbHelper implements DynamodbReadBatch {
    private final DynamodbClient client;

    @Setter
    private int batchSize = 100;
    protected List<OperationsHolder> operations = new ArrayList<>();


    public ReadBatchOperationHelper( DynamodbClient client ) {
        this.client = client;
    }

    @Override
    public void addOperations( List<AbstractOperation> operationsToAdd ) {
        operationsToAdd.forEach( this::addOperation );
    }

    @Override
    public void addOperation( AbstractOperation operation ) {
        List<AbstractOperation> toBeAdded = new ArrayList<>();
        if ( operations.isEmpty() ) {
            operations.add( new OperationsHolder( toBeAdded, false ) );
        } else {
            toBeAdded = operations.get( operations.size() - 1 ).getOperations();
        }
        if ( toBeAdded.size() >= batchSize ) {
            toBeAdded = new ArrayList<>();
            operations.add( new OperationsHolder( toBeAdded, false ) );
        }
        toBeAdded.add( operation );
    }


    public Map<String, Collection<Map<String, AttributeValue>>> read( KeysAndAttributesModifier modifier ) {
        Multimap<String, Map<String, AttributeValue>> result = ArrayListMultimap.create();
        operations.forEach( oh -> {
            final Multimap<String, Map<String, AttributeValue>> keysByTables = ArrayListMultimap.create();
            oh.operations.forEach( operation -> {
                if ( operation.getType() != OperationType.READ ) {
                    throw new UnsupportedOperationException( "only READ operation is supported" );
                }
                operation.setState( DynamodbClient.State.SUCCESS );
                String tableName = operation.getKey().getTableName();
                keysByTables.put( tableName, getKeyAttribute( operation.getKey() ) );
            } );

            Map<String, KeysAndAttributes> requestItems = new HashMap<>();
            keysByTables.asMap().forEach( ( tn, keys ) -> {
                KeysAndAttributes.Builder builder = KeysAndAttributes.builder().keys( keys );
                if ( modifier != null ) {
                    modifier.accept( builder );
                }
                requestItems.put( tn, builder.build() );
            } );
            BatchGetItemRequest.Builder batchGetItemRequest = BatchGetItemRequest.builder().requestItems( requestItems );
            client.readBatch( batchGetItemRequest, null ).forEach( result::putAll );
        } );
        return result.asMap();
    }
}
