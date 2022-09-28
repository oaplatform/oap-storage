package oap.dynamodb.batch;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import oap.dynamodb.crud.AbstractOperation;

import java.util.List;

@Getter
@EqualsAndHashCode
class OperationsHolder {
    final List<AbstractOperation> operations;
    final boolean updateOperation;

    OperationsHolder( List<AbstractOperation> operations, boolean updateOperation ) {
        this.operations = operations;
        this.updateOperation = updateOperation;
    }

    @Override
    public String toString() {
        return operations.stream()
                .map( op -> op.getName() == null ? op.getType() + ":" + op.getKey().getColumnValue() : op.getName() )
                .toList()
                .toString();
    }
}
