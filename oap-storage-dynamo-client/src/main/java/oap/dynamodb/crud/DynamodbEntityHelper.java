package oap.dynamodb.crud;

import lombok.extern.slf4j.Slf4j;
import oap.LogConsolidated;
import oap.dynamodb.DynamodbClient;
import oap.dynamodb.Key;
import oap.dynamodb.annotations.API;
import oap.dynamodb.creator.PojoBeanFromDynamoCreator;
import oap.dynamodb.creator.PojoBeanToDynamoCreator;
import oap.dynamodb.modifiers.GetItemRequestModifier;
import oap.dynamodb.modifiers.UpdateItemRequestModifier;
import oap.util.Pair;
import oap.util.Result;
import org.slf4j.event.Level;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static oap.util.Dates.s;

@Slf4j
@ThreadSafe
public class DynamodbEntityHelper {
    private static final Pattern ATTRIBUTES_PATTERN = Pattern.compile( "(\\w+)(=:\\1(,\\s)?)" );

    private DynamoDbReader reader;
    private DynamoDbWriter writer;

    public DynamodbEntityHelper( DynamoDbReader reader, DynamoDbWriter writer ) {
        this.reader = reader;
        this.writer = writer;
    }

    /**
     * Returns a given entity read from dynamoDB and converted to a given class.
     * @param clazz desired class to be transformed to
     * @param key key of entity
     * @param modifier modifier to sellect fields to read, etc.
     * @return
     * @param <T>
     * @throws ReflectiveOperationException
     */
    @API
    public <T> Result<T, DynamodbClient.State> getItem( Class<T> clazz, Key key, GetItemRequestModifier modifier ) throws ReflectiveOperationException {
        Result<Map<String, AttributeValue>, DynamodbClient.State> result = reader.getRecord( key, modifier );
        if ( result.getFailureValue() != null ) {
            return Result.failure( result.getFailureValue() );
        }
        T item = new PojoBeanFromDynamoCreator<T>().createBean( clazz, result.getSuccessValue() );
        if ( item == null ) {
            return Result.failure( DynamodbClient.State.NOT_FOUND );
        }
        return Result.success( item );
    }

    @API
    public <T> Result<List<T>, DynamodbClient.State> getItems( Class<? extends T> clazz, String tableName, Set<Key> keys, Set<String> attributesToGet ) {
        Result<List<Map<String, AttributeValue>>, DynamodbClient.State> result = reader.getRecords( tableName, keys, attributesToGet );
        if ( result.getFailureValue() != null ) return Result.failure( result.getFailureValue() );
        List<Map<String, AttributeValue>> records = result.successValue;
        var creator = new PojoBeanFromDynamoCreator<T>();
        return Result.success( records
                .stream()
                .map( v -> creator.createBean( clazz, v ) )
                .collect( Collectors.toList() ) );
    }

    /** Writes (or updates existing) a given POJO bean to database. It performs all transformation in order to write.
     *
     * @param key key of DB record
     * @param item POJO bean class to be written
     * @param modifier
     * @return
     */
    @API
    public <T> Result<UpdateItemResponse, DynamodbClient.State> updateOrCreateItem( Key key, T item, UpdateItemRequestModifier modifier ) throws Exception {
        Pair<String, Map<String, AttributeValue>> binNamesAndValues = new PojoBeanToDynamoCreator().createExpressionsFromBean( item, ":" );
        binNamesAndValues._2().remove( ":" + key.getColumnName() ); //remove key from expression

        String expression = binNamesAndValues._1();
        //remove id column
        expression = expression.replace( key.getColumnName() + "=:" + key.getColumnName() + ", ", "" );
        expression = expression.replace( key.getColumnName() + "=:" + key.getColumnName(), "" );
        //process restricted names if any
        Matcher matcher = ATTRIBUTES_PATTERN.matcher( expression );
        StringBuffer safeExpression = new StringBuffer();
        Map<String, String> names = new HashMap<>();
        while( matcher.find() ) {
            matcher.appendReplacement( safeExpression, "#$1$2" );
            names.put( "#" + matcher.group( 1 ), matcher.group( 1 ) );
        }
        matcher.appendTail( safeExpression );

        UpdateItemRequest.Builder updateItemRequest = UpdateItemRequest.builder()
                .tableName( key.getTableName() )
                .key( reader.getKeyAttribute( key ) )
                .updateExpression( "SET " + safeExpression )
                .expressionAttributeNames( names ) //{"#pr":"ProductReviews", "#1star":"OneStar"}
                .expressionAttributeValues( binNamesAndValues._2() )
                .returnValues( "ALL_NEW" );
        if ( modifier != null ) {
            modifier.accept( updateItemRequest );
        }
        try {
            return writer.updateOrCreateItem( updateItemRequest, null );
        } catch( Exception ex ) {
            log.error( "Error in update for key {}", key, ex );
            LogConsolidated.log( log, Level.ERROR, s( 5 ), ex.getMessage(), ex );
            return Result.failure( DynamodbClient.State.ERROR );
        }
    }
}
