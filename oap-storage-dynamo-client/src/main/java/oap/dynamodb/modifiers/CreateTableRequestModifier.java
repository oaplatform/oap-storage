package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;

import java.util.function.Consumer;

/**
 * Allows to modify CreateTableRequest before applying.
 * For instance in order to add StreamAPI:
 *  builder.streamSpecification(StreamSpecification streamSpecification = new StreamSpecification()
 *             .withStreamEnabled(true)
 *             .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES));
 */
@FunctionalInterface
@API
public interface CreateTableRequestModifier extends Consumer<CreateTableRequest.Builder> {
    @Override
    void accept( CreateTableRequest.Builder builder );
}
