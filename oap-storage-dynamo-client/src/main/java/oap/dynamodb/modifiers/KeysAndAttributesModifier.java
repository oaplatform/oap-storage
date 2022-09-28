package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface KeysAndAttributesModifier extends Consumer<KeysAndAttributes.Builder> {
    @Override
    void accept( KeysAndAttributes.Builder builder );
}
