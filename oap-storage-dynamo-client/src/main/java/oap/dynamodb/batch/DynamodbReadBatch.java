package oap.dynamodb.batch;

import oap.dynamodb.crud.AbstractOperation;

import java.util.List;

public interface DynamodbReadBatch {
    void setBatchSize( int batchSize );
    void addOperations( List<AbstractOperation> operations );
    void addOperation( AbstractOperation operation );
}
