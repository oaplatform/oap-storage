package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface ScanRequestModifier extends Consumer<ScanRequest.Builder> {
    @Override
    void accept( ScanRequest.Builder builder );
}
