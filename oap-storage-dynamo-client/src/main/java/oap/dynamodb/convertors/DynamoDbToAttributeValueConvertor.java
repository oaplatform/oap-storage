package oap.dynamodb.convertors;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@FunctionalInterface
public interface DynamoDbToAttributeValueConvertor {
    void convert( AttributeValue.Builder builder, Object value, KeyConvertorForMap convertor );
}
