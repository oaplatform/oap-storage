package oap.dynamodb.creator.samples;

import lombok.Data;

@Data
public class SeveralDifferentFinalFieldInArgsConstructor {
    private final String finalField1;
    private final boolean finalField2;
    private final Integer finalField3;
    private String field;

    public SeveralDifferentFinalFieldInArgsConstructor( String finalField1, Boolean finalField2, int finalField3 ) {
        this.finalField1 = finalField1 + "_second";
        this.finalField2 = finalField2;
        this.finalField3 = finalField3;
    }

    public SeveralDifferentFinalFieldInArgsConstructor( String finalField1, boolean finalField2, Integer finalField3 ) {
        this.finalField1 = finalField1;
        this.finalField2 = finalField2;
        this.finalField3 = finalField3;
    }
}
