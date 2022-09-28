package oap.dynamodb.crud;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.extern.slf4j.Slf4j;
import oap.LogConsolidated;
import oap.dynamodb.DynamodbClient;
import oap.dynamodb.Key;
import oap.dynamodb.annotations.API;
import oap.dynamodb.modifiers.BatchGetItemRequestModifier;
import oap.dynamodb.modifiers.GetItemRequestModifier;
import oap.dynamodb.modifiers.QueryRequestModifier;
import oap.dynamodb.modifiers.ScanRequestModifier;
import oap.util.Result;
import org.slf4j.event.Level;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Strings.isNullOrEmpty;
import static oap.util.Dates.s;

@Slf4j
@ThreadSafe
public class DynamoDbReader extends DynamoDbHelper {

    private final DynamoDbClient dynamoDbClient;
    private final DynamoDbEnhancedClient enhancedClient;
    private final ReentrantReadWriteLock.ReadLock readLock;

    public DynamoDbReader( DynamoDbClient dynamoDbClient, DynamoDbEnhancedClient enhancedClient, ReentrantReadWriteLock.ReadLock readLock ) {
        this.dynamoDbClient = dynamoDbClient;
        this.enhancedClient = enhancedClient;
        this.readLock = readLock;
    }

    @API
    public Result<Map<String, AttributeValue>, DynamodbClient.State> getRecord( Key key, GetItemRequestModifier modifier ) {
        //read item by key
        GetItemRequest.Builder getItemRequest = GetItemRequest.builder()
            .key( getKeyAttribute( key ) )
            .tableName( key.getTableName() );
        try {
            if ( modifier != null ) {
                modifier.accept( getItemRequest );
            }
            readLock.lock();
            GetItemResponse response = dynamoDbClient.getItem( getItemRequest.build() );
            if ( !response.hasItem() ) return Result.failure( DynamodbClient.State.NOT_FOUND );
            return Result.success( response.item() );
        } catch( Exception ex ) {
            log.error( "Error in get", ex );
            LogConsolidated.log( log, Level.ERROR, s( 5 ), ex.getMessage(), ex );
            return Result.failure( DynamodbClient.State.ERROR );
        } finally {
            readLock.unlock();
        }
    }

    @API
    public Result<List<Map<String, AttributeValue>>, DynamodbClient.State> getRecords( String tableName, Set<Key> keys, Set<String> attributesToGet ) {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        List<Map<String, AttributeValue>> allKeys = keys
            .stream()
            .map( key -> getKeyAttribute( key ) )
            .collect( Collectors.toList() );

        KeysAndAttributes.Builder keysAndAttributes = KeysAndAttributes
            .builder()
            .consistentRead( true )
            .keys( allKeys );
        if( attributesToGet != null ) {
            keysAndAttributes.attributesToGet( attributesToGet ); //projection
        }
        requestItems.put( tableName, keysAndAttributes.build() );
        BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest
            .builder()
            .requestItems( requestItems )
            .build();
        try {
            readLock.lock();
            BatchGetItemResponse response = dynamoDbClient.batchGetItem( batchGetItemRequest );
            List<Map<String, AttributeValue>> result = new ArrayList<>();
            if( response.hasResponses() ) {
                response.responses().forEach( ( name, attributeValues ) -> {
                    Map<String, AttributeValue> values = new HashMap<>();
                    attributeValues.stream().forEach( map -> map.entrySet().stream().forEach( entry -> values.put( entry.getKey(), entry.getValue() ) ) );
                    result.add( values );
                } );
                return Result.success( result );
            }
        } finally {
            readLock.unlock();
        }
        return Result.failure( DynamodbClient.State.NOT_FOUND );
    }

    @API
    public ItemsPage getRecords( String tableName,
                                int pageSize,
                                String columnName,
                                String exclusiveStartItemId,
                                ScanRequestModifier modifier ) {
        try {
            readLock.lock();
            ScanRequest.Builder scanRequest = ScanRequest.builder()
                    .tableName( tableName )
                    .limit( pageSize );
            if( !isNullOrEmpty( exclusiveStartItemId ) ) {
                scanRequest.exclusiveStartKey( Collections.singletonMap( columnName, AttributeValue.builder().s( exclusiveStartItemId ).build() ) );
            }
            if( modifier != null ) {
                modifier.accept( scanRequest );
            }
            ScanResponse result = dynamoDbClient.scan( scanRequest.build() );
            List<Map<String, AttributeValue>> records = result.items();

            ItemsPage.ItemsPageBuilder builder = ItemsPage.builder().records( records );
            if ( result.lastEvaluatedKey() != null && !result.lastEvaluatedKey().isEmpty() ) {
                if ( !result.lastEvaluatedKey().containsKey( columnName ) || isNullOrEmpty( result.lastEvaluatedKey().get( columnName ).s() ) ) {
                    throw new IllegalStateException( "orderId did not exist or was not a non-empty string in the lastEvaluatedKey" );
                } else {
                    builder.lastEvaluatedKey( result.lastEvaluatedKey().get( columnName ).s() );
                }
            }
            return builder.build();
        } catch ( ResourceNotFoundException e ) {
            throw new RuntimeException( "Order table " + tableName + " does not exist" );
        } finally {
            readLock.unlock();
        }
    }

    public Map<String, Collection<Map<String, AttributeValue>>> batchGetRecords( BatchGetItemRequest.Builder batchGetItemRequest, BatchGetItemRequestModifier modifier ) {
        Objects.requireNonNull( batchGetItemRequest );
        if ( modifier != null ) {
            modifier.accept( batchGetItemRequest );
        }
        try {
            readLock.lock();
            BatchGetItemResponse outcome = dynamoDbClient.batchGetItem( batchGetItemRequest.build() );

            Map<String, KeysAndAttributes> unprocessed = null;
            Multimap<String, Map<String, AttributeValue>> result = ArrayListMultimap.create();
            do {
                for ( String tableName : outcome.responses().keySet() ) {
                    List<Map<String, AttributeValue>> items = outcome.responses().get( tableName );
                    result.putAll( tableName, items );
                }
                // Check for unprocessed keys which could happen if you exceed
                // provisioned
                // throughput or reach the limit on response size.
                unprocessed = outcome.unprocessedKeys();
                if ( !unprocessed.isEmpty() ) {
                    batchGetItemRequest.requestItems( unprocessed );
                    outcome = dynamoDbClient.batchGetItem( batchGetItemRequest.build() );
                }
            } while ( !unprocessed.isEmpty() );
            return result.asMap();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Scans a given table and provide a result as a stream of records. You may control the scan by using index as well as projection.
     * @param tableName
     * @param modifier
     * @return
     */
    @API
    public Stream<Map<String, AttributeValue>> getRecordsByScan( String tableName, ScanRequestModifier modifier ) {
        ScanRequest.Builder scanRequest = ScanRequest.builder().tableName( tableName );
        if( modifier != null ) {
            modifier.accept( scanRequest );
        }
        try {
            readLock.lock();
            ScanIterable result = dynamoDbClient.scanPaginator( scanRequest.build() );
            return result.items().stream();
        } finally {
            readLock.unlock();
        }
    }

    @API
    public Stream<Map<String, AttributeValue>> getRecordsByQuery( String tableName, Key key, QueryRequestModifier modifier ) {
        QueryRequest.Builder queryRequest = QueryRequest.builder()
            .tableName( tableName )
            .keyConditionExpression( key.getColumnName() + " = :pkval" )
            .expressionAttributeValues( Map.of( ":pkval", attr( generateKeyValue( key ) ) ) );
        if( modifier != null ) modifier.accept( queryRequest );
        try {
            readLock.lock();
            QueryIterable result = dynamoDbClient.queryPaginator( queryRequest.build() );
            return result.items().stream();
        } finally {
            readLock.unlock();
        }
    }

    @API
    public Stream<Map<String, AttributeValue>> getRecordsByQuery( String tableName,
                                                                  String indexName,
                                                                  String filterExpression,
                                                                  QueryRequestModifier modifier ) {
        QueryRequest.Builder queryRequest = QueryRequest.builder()
                .tableName( tableName )
                .indexName( indexName )
                .filterExpression( filterExpression );
        if( modifier != null ) {
            modifier.accept( queryRequest );
        }
        try {
            readLock.lock();
            QueryIterable result = dynamoDbClient.queryPaginator( queryRequest.build() );
            return result.items().stream();
        } finally {
            readLock.unlock();
        }
    }

    @API
    public Stream<Map<String, AttributeValue>> getRecordsByQuery( String tableName,
                                                                 Key key,
                                                                 String sortKeyName,
                                                                 String sortKeyValue,
                                                                 QueryRequestModifier modifier ) {
        QueryRequest.Builder queryRequest = QueryRequest.builder()
            .tableName( tableName )
            .keyConditionExpression( key.getColumnName() + " = :pkval and " + sortKeyName + " = :skVal" )
            .expressionAttributeValues( Map.of( ":pkval", attr( generateKeyValue( key ) ), ":skval", attr( sortKeyValue ) ) );
        if( modifier != null ) {
            modifier.accept( queryRequest );
        }
        QueryIterable result = dynamoDbClient.queryPaginator( queryRequest.build() );
        return result.items().stream();
    }

    private static AttributeValue attr( int n ) {
        return AttributeValue.builder().n( String.valueOf( n ) ).build();
    }

    private static AttributeValue attr( String s ) {
        return AttributeValue.builder().s( s ).build();
    }
}
