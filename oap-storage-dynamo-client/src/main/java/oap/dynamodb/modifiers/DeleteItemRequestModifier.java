package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface DeleteItemRequestModifier extends Consumer<DeleteItemRequest.Builder> {
    @Override
    void accept( DeleteItemRequest.Builder builder );
}
