package oap.dynamodb;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.dynamodb.annotations.API;

import java.util.Objects;

@Data
@EqualsAndHashCode
@ToString
@API
public class KeyForSchema {
    private final String tableName;
    private final String columnName;

    public KeyForSchema( String tableName, String columnName ) {
        Objects.requireNonNull( tableName );
        Objects.requireNonNull( columnName );
        this.tableName = tableName;
        this.columnName = columnName;
    }
}
