package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface DescribeTableResponseModifier extends Consumer<DescribeTableRequest.Builder> {
    @Override
    void accept( DescribeTableRequest.Builder builder );
}
