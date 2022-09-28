package oap.dynamodb.creator.samples;

import lombok.Data;

@Data
public class FinalFieldInArgsConstructor {
    private final String finalField;
    private String field;

    public FinalFieldInArgsConstructor( String field ) {
        this.finalField = field;
    }
}
