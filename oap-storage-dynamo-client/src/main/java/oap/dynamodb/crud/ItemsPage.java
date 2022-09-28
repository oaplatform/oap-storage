package oap.dynamodb.crud;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

@Builder
@Getter
@ToString
public class ItemsPage {
    private List<Map<String, AttributeValue>> records;
    private String lastEvaluatedKey;
}
