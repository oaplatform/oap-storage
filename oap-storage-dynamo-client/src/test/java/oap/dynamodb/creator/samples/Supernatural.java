package oap.dynamodb.creator.samples;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
class Supernatural {
    private static String staticVar;
    protected String superVar;
}
