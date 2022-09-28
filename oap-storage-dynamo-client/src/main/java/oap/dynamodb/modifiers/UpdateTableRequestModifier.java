package oap.dynamodb.modifiers;

import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

import java.util.function.Consumer;

@FunctionalInterface
public interface UpdateTableRequestModifier extends Consumer<UpdateTableRequest.Builder> {
    @Override
    void accept( UpdateTableRequest.Builder builder );
}
