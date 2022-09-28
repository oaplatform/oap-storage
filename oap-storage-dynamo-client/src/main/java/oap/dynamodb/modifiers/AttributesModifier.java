package oap.dynamodb.modifiers;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;
import java.util.function.Function;

public interface AttributesModifier extends Function<Map<String, AttributeValue>, Map<String, AttributeValue>> {

    /**
     * Note: argument is UnmodifiableMap, so you have to create a new one in order to update something inside.
     */
    @Override
    Map<String, AttributeValue> apply( Map<String, AttributeValue> valueMap );
}
