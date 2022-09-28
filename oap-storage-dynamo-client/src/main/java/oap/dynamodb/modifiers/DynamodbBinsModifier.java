package oap.dynamodb.modifiers;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;
import java.util.function.Consumer;

@FunctionalInterface
public interface DynamodbBinsModifier extends Consumer<Map<String, AttributeValue>> {
    @Override
    void accept( Map<String, AttributeValue> args );
}
