package oap.dynamodb.creator.samples;

import lombok.Data;
import lombok.ToString;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

import java.util.List;
import java.util.Map;

@Data
@ToString( exclude = "listOfBinaries", callSuper = true )
@DynamoDbBean
public class AutonomiousDynamo extends Supernatural {
    private static String staticVar;
    private String id;
    private final String finalVar;
    public Long publicLongVar;
    public Integer publicIntVar;
    public Float publicFloatVar;
    public Double publicDoubleVar;
    public String publicStringVar;
    public Boolean publicBooleanVar;
    public byte[] publicBytesVar;

    private Integer intVar;
    private Long longVar;
    private Float floatVar;
    private Double doubleVar;
    private String stringVar;
    private Double numberVar; // does not work with interface for field definition like Number
    private Boolean booleanVar;
    private byte[] bytesVar;
    private List<String> listOfStrings;
    private List<byte[]> listOfBinaries;
    private List<Integer> listOfIntegers;
    private Map<String, List<String>> mapOfObjects; // does not work with Object as values

    @DynamoDbPartitionKey
    public String getId() {
        return id;
    }

    public AutonomiousDynamo() {
        this.finalVar = "field is not set";
    }
    public AutonomiousDynamo( String finalVar ) {
        this.finalVar = finalVar;
    }
}
