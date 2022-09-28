package oap.dynamodb.creator.samples;

import lombok.Data;
import lombok.ToString;

import java.util.List;

@Data
@ToString
public class TestClassWithListOfLongs {
    private List<Long> field;
}
