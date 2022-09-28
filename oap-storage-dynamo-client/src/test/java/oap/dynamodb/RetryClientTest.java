package oap.dynamodb;

import oap.application.Kernel;
import oap.application.module.Module;
import oap.dynamodb.modifiers.GetItemRequestModifier;
import oap.dynamodb.modifiers.UpdateItemRequestModifier;
import oap.testng.Fixtures;
import oap.util.HashMaps;
import oap.util.Result;
import oap.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static oap.testng.Asserts.pathOfResource;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Ignore
public class RetryClientTest  extends Fixtures {

    public static final String TABLE_NAME = "retryTest";
    public static final String ID_COLUMN_NAME = "id";

    private final AbstractDynamodbFixture fixture = new TestContainerDynamodbFixture();

    public RetryClientTest() {
        fixture( fixture );
        var kernel = new Kernel( Module.CONFIGURATION.urlsFromClassPath() );
        kernel.start( pathOfResource( getClass(), "/oap/dynamodb/test-application.conf" ) );
    }

    private AtomicInteger counter = new AtomicInteger();
    private Map<String, AttributeValue> attributeValueMap = HashMaps.of(
            "generation", AttributeValue.fromN( "2" ),
            "bin1", AttributeValue.fromS( "Adam Smith" ),
            "bin2", AttributeValue.fromS( "Samuel Collins" )
    );

    @NotNull
    private DynamodbClient createClient() {
        return new DynamodbClient( fixture.getDynamodbClient().getDynamoDbClient() ) {
            public Result<Map<String, AttributeValue>, State> getRecord( Key key, GetItemRequestModifier modifier ) {
                return Result.success( attributeValueMap );
            }
            public Result<UpdateItemResponse, DynamodbClient.State> updateRecordAtomic( Key key, Map<String, AttributeValue> binNamesAndValues, UpdateItemRequestModifier modifier, int generation ) {
                counter.incrementAndGet();
                if ( counter.get() == 5 ) {
                    attributeValueMap.put( "generation", AttributeValue.fromN( "2" ) );
                    attributeValueMap.put( "bin3", AttributeValue.fromS( "v2" ) );
                    return Result.success( UpdateItemResponse.builder().attributes( attributeValueMap ).build() );
                }
                return Result.failure( State.VERSION_CHECK_FAILED );
            }
        };
    }

    @Test
    public void atomicUpdateWithRetry() throws IOException {
        var client = createClient();

        client.start();
        client.waitConnectionEstablished();
        client.deleteTableIfExists( TABLE_NAME );
        client.createTableIfNotExist( TABLE_NAME, ID_COLUMN_NAME );
        Key key = new Key( TABLE_NAME, ID_COLUMN_NAME, "Palo Alto, CA" );
        //attempt to write v1, but there actually is v2, so we have to repeat 5 attempts
        Map<String, AttributeValue> attributes = Collections.singletonMap( "bin3", AttributeValue.fromS( "v1" ) );

        Result<UpdateItemResponse, DynamodbClient.State> result = client.updateRecordAtomicWithRetry( key,
                Sets.of( "bin1", "bin2" ),
                attributeValueMap -> {
                    Map<String, AttributeValue> map = new HashMap<>( attributeValueMap );
                    map.putAll( attributes );
                    return attributes;
                },
                5,
                1 );

        assertThat( counter.get() ).isEqualTo( 5 );
        assertThat( result.isSuccess() ).isTrue();
        assertThat( result.getSuccessValue().attributes().get( "bin3" ).s() ).isEqualTo( "v2" );
        assertThat( result.getSuccessValue().attributes().get( "generation" ).n() ).isEqualTo( "2" );
    }
}
