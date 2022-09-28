package oap.dynamodb.creator.samples;

import lombok.Data;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Data
@ToString( exclude = "listOfBinaries", callSuper = true )
public class Autonomious extends Supernatural {
    private static String staticVar;
    private String id;
    private final String finalVar;
    public Long publicLongVar;
    public Integer publicIntVar;
    public Float publicFloatVar;
    public Double publicDoubleVar;
    public String publicStringVar;
    public Boolean publicBooleanVar;
    public Number publicNumberVar;
    public byte[] publicBytesVar;

    private Integer intVar;
    private Long longVar;
    private Float floatVar;
    private Double doubleVar;
    private String stringVar;
    private Number numberVar;
    private Boolean booleanVar;
    private byte[] bytesVar;
    private List<String> listOfStrings;
    private List<byte[]> listOfBinaries;
    private List<Integer> listOfIntegers;
    private Map<String, Object> mapOfObjects;

    public Autonomious( String finalVar ) {
        this.finalVar = finalVar;
    }

}
