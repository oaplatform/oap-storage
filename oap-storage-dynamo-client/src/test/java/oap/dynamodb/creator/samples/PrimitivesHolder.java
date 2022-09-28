package oap.dynamodb.creator.samples;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString
@EqualsAndHashCode
public class PrimitivesHolder {
    private boolean booleanVar;
    private int intVar;
    private long longVar;
    private float floatVar;
    private double doubleVar;
}
