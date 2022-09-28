package oap.dynamodb.creator.samples;

import lombok.Data;
import lombok.ToString;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

@Data
@ToString
@DynamoDbBean
public class EmbeddedBean {
    private String embeddedFieldString;
    private int embeddedFieldInt;

    public EmbeddedBean() {
    }
}
