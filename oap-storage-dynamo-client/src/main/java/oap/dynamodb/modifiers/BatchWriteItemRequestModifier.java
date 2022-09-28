package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface BatchWriteItemRequestModifier extends Consumer<BatchWriteItemRequest.Builder> {
    @Override
    void accept( BatchWriteItemRequest.Builder builder );
}
