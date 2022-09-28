package oap.dynamodb.modifiers;

import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;

import java.util.function.Consumer;

@FunctionalInterface
public interface CreateGlobalSecondaryIndexActionModifier extends Consumer<CreateGlobalSecondaryIndexAction.Builder> {
    @Override
    void accept( CreateGlobalSecondaryIndexAction.Builder builder );
}
