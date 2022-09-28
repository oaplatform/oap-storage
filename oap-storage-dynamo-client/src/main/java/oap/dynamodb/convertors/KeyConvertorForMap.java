package oap.dynamodb.convertors;

@FunctionalInterface
public interface KeyConvertorForMap {
    Object convert( Object obj );
}
