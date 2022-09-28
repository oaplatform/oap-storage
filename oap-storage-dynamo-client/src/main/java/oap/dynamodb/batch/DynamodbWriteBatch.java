package oap.dynamodb.batch;

import oap.dynamodb.annotations.API;
import oap.dynamodb.crud.AbstractOperation;

import java.util.List;

@API
public interface DynamodbWriteBatch {
    void setBatchSize( int batchSize );
    void addOperations( List<AbstractOperation> operations );
    void addOperation( AbstractOperation operation );
    boolean write();
}
