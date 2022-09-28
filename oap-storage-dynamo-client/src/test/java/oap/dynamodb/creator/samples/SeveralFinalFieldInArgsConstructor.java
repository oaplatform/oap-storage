package oap.dynamodb.creator.samples;

import lombok.Data;

@Data
public class SeveralFinalFieldInArgsConstructor {
    private final String finalField1;
    private final String finalField2;
    private final String finalField3;
    private String field;

    public SeveralFinalFieldInArgsConstructor( String finalField1, String finalField2, String finalField3 ) {
        this.finalField1 = finalField1;
        this.finalField2 = finalField2;
        this.finalField3 = finalField3;
    }
}
