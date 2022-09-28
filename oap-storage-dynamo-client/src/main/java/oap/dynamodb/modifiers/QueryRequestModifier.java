package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface QueryRequestModifier extends Consumer<QueryRequest.Builder> {
    @Override
    void accept( QueryRequest.Builder builder );
}
