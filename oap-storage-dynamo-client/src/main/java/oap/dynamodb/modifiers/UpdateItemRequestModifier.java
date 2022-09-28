package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface UpdateItemRequestModifier extends Consumer<UpdateItemRequest.Builder> {
    @Override
    void accept( UpdateItemRequest.Builder builder );
}
