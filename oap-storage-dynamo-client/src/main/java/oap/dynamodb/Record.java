package oap.dynamodb;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Data
@EqualsAndHashCode
@ToString
public class Record {

    private String id;
    private String sortKey;

    @DynamoDbPartitionKey
    @DynamoDbAttribute( "id" )
    public String getId() {
        return id;
    }

    @DynamoDbSortKey
    @DynamoDbAttribute( "sortKey" )
    public String getSortKey() {
        return sortKey;
    }

}
