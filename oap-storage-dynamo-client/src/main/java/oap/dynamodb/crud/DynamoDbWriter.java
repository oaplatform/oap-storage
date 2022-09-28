package oap.dynamodb.crud;

import lombok.extern.slf4j.Slf4j;
import oap.LogConsolidated;
import oap.dynamodb.DynamodbClient;
import oap.dynamodb.Key;
import oap.dynamodb.annotations.API;
import oap.dynamodb.convertors.DynamodbDatatype;
import oap.dynamodb.modifiers.BatchWriteItemRequestModifier;
import oap.dynamodb.modifiers.DeleteItemRequestModifier;
import oap.dynamodb.modifiers.UpdateItemRequestModifier;
import oap.util.Result;
import org.slf4j.event.Level;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static oap.util.Dates.s;

@Slf4j
@ThreadSafe
public class DynamoDbWriter extends DynamoDbHelper {
    private final DynamoDbClient dynamoDbClient;
    private final DynamoDbEnhancedClient enhancedClient;
    private final ReentrantReadWriteLock.ReadLock readLock;

    public DynamoDbWriter( DynamoDbClient dynamoDbClient, DynamoDbEnhancedClient enhancedClient, ReentrantReadWriteLock.ReadLock readLock ) {
        this.dynamoDbClient = dynamoDbClient;
        this.enhancedClient = enhancedClient;
        this.readLock = readLock;
    }

    @API
    public <T> Result<UpdateItemResponse, DynamodbClient.State> updateOrCreateItem( UpdateItemRequest.Builder updateItemRequest, UpdateItemRequestModifier modifier ) {
        if ( modifier != null ) {
            modifier.accept( updateItemRequest );
        }
        UpdateItemRequest itemRequest = updateItemRequest.build();
        try {
            readLock.lock();
            UpdateItemResponse response = dynamoDbClient.updateItem( itemRequest );
            return Result.success( response );
        } catch( Exception ex ) {
            log.error( "Error in update for key {}", itemRequest.key(), ex );
            LogConsolidated.log( log, Level.ERROR, s( 5 ), ex.getMessage(), ex );
            return Result.failure( DynamodbClient.State.ERROR );
        } finally {
            readLock.unlock();
        }
    }

    @API
    public Result<UpdateItemResponse, DynamodbClient.State> updateRecord( Key key, Map<String, AttributeValue> binNamesAndValues, UpdateItemRequestModifier modifier ) {
        Map<String, AttributeValueUpdate> updatedValues = new HashMap<>();

        binNamesAndValues.forEach( ( binName, binValue ) -> {
            // skip key
            if ( key.getColumnName().equals( binName ) ) return;
            // Update the column specified by name with updatedVal
            if( binValue != null ) {
                updatedValues.put( binName, AttributeValueUpdate.builder()
                        .value( binValue )
                        .action( AttributeAction.PUT )
                        .build() );
            } else {
                updatedValues.put( binName, AttributeValueUpdate.builder()
                        .action( AttributeAction.DELETE )
                        .build() );
            }
        } );
        UpdateItemRequest.Builder updateItemRequest = UpdateItemRequest.builder()
                .tableName( key.getTableName() )
                .key( getKeyAttribute( key ) )
                .attributeUpdates( updatedValues );
        if ( modifier != null ) {
            modifier.accept( updateItemRequest );
        }
        try {
            readLock.lock();
            UpdateItemResponse response = dynamoDbClient.updateItem( updateItemRequest.build() );
            return Result.success( response );
        } catch ( ConditionalCheckFailedException ex ) {
            //in case of atomic update this could happen if record has a version which does not fit with a given version (a.k.a. generation)
            return Result.failure( DynamodbClient.State.VERSION_CHECK_FAILED );
        } catch( Exception ex ) {
            log.error( "Error in update for key {}", key, ex );
            LogConsolidated.log( log, Level.ERROR, s( 5 ), ex.getMessage(), ex );
            return Result.failure( DynamodbClient.State.ERROR );
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Note: to make an atomic update see .action( AttributeAction.ADD ) for numeric or set datatypes
     * @param key
     * @param binNamesAndValues
     * @param modifier
     * @return
     */
    @API
    public Result<UpdateItemResponse, DynamodbClient.State> update( Key key, Map<String, Object> binNamesAndValues, UpdateItemRequestModifier modifier ) {
        Map<String, AttributeValue> updatedValues = new HashMap<>();
        StringBuilder setExpression = new StringBuilder();
        AtomicInteger counter = new AtomicInteger();
        binNamesAndValues.forEach( ( binName, binValue ) -> {
            // Update the column specified by name with updatedVal
            int number = counter.incrementAndGet();
            updatedValues.put( ":var" + number, binValue != null
                    ? DynamodbDatatype.createAttributeValueFromObject( binValue )
                    : AttributeValue.fromNul( true ) );
            setExpression.append( binName + " = :var" + number + ", " );
        } );
        if( !setExpression.isEmpty() ) setExpression.setLength( setExpression.length() - ", ".length() );
        String updateExpression = setExpression.isEmpty() ? "" : "SET " + setExpression.toString();
        UpdateItemRequest.Builder updateItemRequest = UpdateItemRequest.builder()
            .tableName( key.getTableName() )
            .key( getKeyAttribute( key ) )
            .updateExpression( updateExpression )
            .expressionAttributeValues( updatedValues );
        if ( modifier != null ) {
            modifier.accept( updateItemRequest );
        }
        try {
            readLock.lock();
            UpdateItemResponse response = dynamoDbClient.updateItem( updateItemRequest.build() );
            return Result.success( response );
        } catch( Exception ex ) {
            log.error( "Error in update for key {}", key, ex );
            LogConsolidated.log( log, Level.ERROR, s( 5 ), ex.getMessage(), ex );
            return Result.failure( DynamodbClient.State.ERROR );
        } finally {
            readLock.unlock();
        }
    }

    @API
    public Result<DynamodbClient.State, DynamodbClient.State> updateUsingGetAndPut( String tableName,
                                                                                    String keyName,
                                                                                    String keyValue,
                                                                                    String binName,
                                                                                    Object binValue ) {
        //read item by key
        Key key = new Key( tableName, keyName, keyValue );
        GetItemRequest getItemRequest = GetItemRequest.builder()
            .key( getKeyAttribute( key ) )
            .tableName( tableName )
            .build();
        try {
            readLock.lock();
            try {
                Map<String, AttributeValue> oldValues = dynamoDbClient.getItem( getItemRequest ).item();
                Map<String, AttributeValue> newValues = generateBinNamesAndValues( key, binName, binValue, oldValues );
                //write item
                PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName( tableName )
                    .item( newValues )
                    .build();

                dynamoDbClient.putItem( putItemRequest );
                return Result.success( DynamodbClient.State.SUCCESS );
            } catch( Exception ex ) {
                log.error( "Error in put", ex );
                LogConsolidated.log( log, Level.ERROR, s( 5 ), ex.getMessage(), ex );
                return Result.failure( DynamodbClient.State.ERROR );
            }
        } finally {
            readLock.unlock();
        }
    }

    @API
    public Result<Map<String, AttributeValue>, DynamodbClient.State> delete( Key key, DeleteItemRequestModifier modifier ) {
        try {
            DeleteItemRequest.Builder deleteItemRequest = DeleteItemRequest.builder().tableName( key.getTableName() );
            deleteItemRequest.key( getKeyAttribute( key ) );
            if ( modifier != null ) {
                modifier.accept( deleteItemRequest );
            }
            readLock.lock();
            try {
                DeleteItemResponse response = dynamoDbClient.deleteItem( deleteItemRequest.build() );
                return Result.success( response.attributes() );
            } catch( Exception ex ) {
                log.error( "Error in put", ex );
                LogConsolidated.log( log, Level.ERROR, s( 5 ), ex.getMessage(), ex );
                return Result.failure( DynamodbClient.State.ERROR );
            }
        } finally {
            readLock.unlock();
        }
    }

    @API
    public BatchWriteItemResponse writeBatch( BatchWriteItemRequest.Builder builder,  BatchWriteItemRequestModifier modifier ) {
        if ( modifier != null ) {
            modifier.accept( builder );
        }
        try {
            readLock.lock();
            return dynamoDbClient.batchWriteItem( builder.build() );
        } finally {
            readLock.unlock();
        }
    }

    public BatchWriteItemResponse writeBatchUnprocessed( Map<String, List<WriteRequest>> unprocessed ) {
        try {
            readLock.lock();
            return dynamoDbClient.batchWriteItem( BatchWriteItemRequest.builder().requestItems( unprocessed ).build() );
        } finally {
            readLock.unlock();
        }
    }
}

