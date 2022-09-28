package oap.dynamodb.creator.samples;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;

public class UsingIgnoreFieldAnno {
    private String field;
    private String ignoreField;

    public String getField() {
        return field;
    }

    public void setField( String field ) {
        this.field = field;
    }

    @DynamoDbIgnore
    public String getIgnoreField() {
        return ignoreField;
    }

    public void setIgnoreField( String ignoreField ) {
        this.ignoreField = ignoreField;
    }
}
