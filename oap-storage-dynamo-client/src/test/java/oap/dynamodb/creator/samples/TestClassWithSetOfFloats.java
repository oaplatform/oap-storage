package oap.dynamodb.creator.samples;

import lombok.Data;
import lombok.ToString;

import java.util.Set;

@Data
@ToString
public class TestClassWithSetOfFloats {
    private Set<Float> field;
}
