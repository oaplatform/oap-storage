package oap.dynamodb.creator.samples;

import lombok.Data;
import lombok.ToString;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

@Data
@ToString
@DynamoDbBean
public class CompositeBean {
    private String compositeFieldString;
    private EmbeddedBean embeddedBean;
    private boolean compositeFieldBoolean;

    public CompositeBean() {
    }
}
