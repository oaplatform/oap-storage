package oap.dynamodb.modifiers;

import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

import java.util.function.Consumer;

@FunctionalInterface
public interface GetItemRequestModifier extends Consumer<GetItemRequest.Builder> {
    @Override
    void accept( GetItemRequest.Builder builder );
}
