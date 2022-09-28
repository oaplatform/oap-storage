package oap.dynamodb.streams;

import oap.dynamodb.annotations.API;
import software.amazon.awssdk.services.dynamodb.model.Record;

import java.util.function.Consumer;

@FunctionalInterface
@API
public interface RecordWorker extends Consumer<Record> {

    @Override
    void accept( Record record );
}
