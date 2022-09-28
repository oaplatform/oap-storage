package oap.dynamodb;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import oap.dynamodb.annotations.API;

import java.util.Objects;

@Getter
@ToString( callSuper = true )
@EqualsAndHashCode( callSuper = true )
@API
public class Key extends KeyForSchema {
    private final String columnValue;

    public Key( String tableName, String columnName, String columnValue ) {
        super( tableName, columnName );
        Objects.requireNonNull( columnValue );
        this.columnValue = columnValue;
    }
}
