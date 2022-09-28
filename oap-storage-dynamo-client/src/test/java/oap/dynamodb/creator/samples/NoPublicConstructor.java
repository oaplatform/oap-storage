package oap.dynamodb.creator.samples;

import lombok.Data;

@Data
public class NoPublicConstructor {
    private String field;

    private NoPublicConstructor() {
    }
}
