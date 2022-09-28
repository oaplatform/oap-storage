package oap.dynamodb.convertors;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@FunctionalInterface
public interface DynamoDBFromAttributeValueConvertor {
    Object convert( AttributeValue attributeValue, KeyConvertorForMap convertor );
}
