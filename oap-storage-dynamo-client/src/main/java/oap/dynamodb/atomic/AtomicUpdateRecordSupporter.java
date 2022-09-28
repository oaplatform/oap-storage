package oap.dynamodb.atomic;

import oap.dynamodb.annotations.API;
import oap.dynamodb.modifiers.UpdateItemRequestModifier;
import oap.util.HashMaps;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@API
public class AtomicUpdateRecordSupporter implements UpdateItemRequestModifier {
    private String recordVersionColumnName = "generation";
    private long generation = 0;
    private Map<String, AttributeValue> atomicUpdates = new LinkedHashMap<>();

    public AtomicUpdateRecordSupporter() {
    }

    public void addAtomicUpdateFor( String binName, AttributeValue binValue ) {
        Objects.requireNonNull( binName );
        Objects.requireNonNull( binValue );
        atomicUpdates.put( binName, binValue );
    }

    /**
     * Prepares UpdateItemRequest for updating a record in DynamoDB. It also adds a feature to support read/check/write
     * functionality with #recordVersionColumnName field (a.k.a. generation or version). If a record in DynamoDB
     * has such field the given value ('generation') should be equal to DynamoDB field value, otherwise
     * update will fail with ConditionalCheckFailedException
     * @param builder an UpdateItemRequest.Builder to modify
     * Note: generation version number of a record to be equal in order to make update happen
     */
    @Override
    public void accept( UpdateItemRequest.Builder builder ) {
        Objects.requireNonNull( builder );
        StringBuilder toSetExpression = new StringBuilder();
        Map<String, String> expressionAttributeNames = HashMaps.of( "#gen", recordVersionColumnName );
        Map<String, AttributeValue> expressionAttributeValues = HashMaps.of(
                ":inc", AttributeValue.fromN( "1" ),
                ":null", AttributeValue.fromNul( true ),
                ":gen", AttributeValue.fromN( String.valueOf( generation ) ) );

        int counter = 0;
        for ( Map.Entry<String, AttributeValue> atomicUpdate :  atomicUpdates.entrySet() ) {
            toSetExpression.append( "#var" + counter + " = :var" + counter + ", " );
            expressionAttributeNames.put( "#var" + counter, atomicUpdate.getKey() );
            expressionAttributeValues.put( ":var" + counter, atomicUpdate.getValue() );
            counter++;
        }
        if ( toSetExpression.length() == 0 ) {
            throw new IllegalArgumentException( "You have not added any atomic update instruction via #addAtomicUpdateFor method" );
        }
        toSetExpression.setLength( toSetExpression.length() - ", ".length() );
        builder
                .attributeUpdates( null ) //these (old, obsolete) updates are not compatible with given pairs
                .conditionExpression( "attribute_not_exists(#gen) OR (attribute_exists(#gen) AND (#gen = :gen OR #gen = :null))" )
                .updateExpression( "SET " + toSetExpression + " ADD #gen :inc" )
                .expressionAttributeNames( expressionAttributeNames )
                .expressionAttributeValues( expressionAttributeValues );
    }

    public void setRecordVersionColumnName( String recordVersionColumnName ) {
        Objects.requireNonNull( recordVersionColumnName );
        this.recordVersionColumnName = recordVersionColumnName;
    }

    public void setGeneration( long generation ) {
        if ( generation < 0 ) {
            this.generation = 0;
            return;
        }
        this.generation = generation;
    }
}
