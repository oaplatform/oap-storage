package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface BatchGetItemRequestModifier extends Consumer<BatchGetItemRequest.Builder> {
    @Override
    void accept( BatchGetItemRequest.Builder builder );
}
