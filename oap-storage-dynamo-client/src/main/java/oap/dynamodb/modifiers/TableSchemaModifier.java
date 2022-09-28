package oap.dynamodb.modifiers;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;

import java.util.function.Consumer;

/**
 * Simple attribute example:
 *      builder.addAttribute(String.class, a -> a.name("attribute")
 *                         .getter(T::getAttribute)
 *                         .setter(T::setAttribute))
 *
 * Secondary partition key attribute example:
 *      builder.addAttribute(String.class, a -> a.name("attribute2*")
 *                         .getter(T::getAttribute2)
 *                         .setter(T::setAttribute2)
 *                         .tags(secondaryPartitionKey("gsi_1")))
 *
 * Secondary sort key attribute example:
 *      builder.addAttribute(String.class, a -> a.name(ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS)
 *                         .getter(T::getAttribute3)
 *                         .setter(T::setAttribute3)
 *                         .tags(secondarySortKey("gsi_1")))
 *
 *
 *  Where T type is extending type for a Record type.
 * @param <T>
 */
@FunctionalInterface
@API
public interface TableSchemaModifier<T> extends Consumer<StaticTableSchema.Builder<T>> {
    @Override
    void accept( StaticTableSchema.Builder<T> builder );
}
