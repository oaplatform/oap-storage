package oap.dynamodb.convertors;

@FunctionalInterface
public interface DynamoDbAttributeValueSizeCalculator {
    int size( Object obj );
}
