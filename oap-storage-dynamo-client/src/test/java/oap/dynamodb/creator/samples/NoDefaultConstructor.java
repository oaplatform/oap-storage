package oap.dynamodb.creator.samples;

import lombok.Data;

@Data
public class NoDefaultConstructor {
    private String field;

    public NoDefaultConstructor( String field ) {
        this.field = field;
    }
}
