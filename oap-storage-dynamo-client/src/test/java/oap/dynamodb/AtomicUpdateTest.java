package oap.dynamodb;

import oap.application.Kernel;
import oap.application.module.Module;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import oap.util.HashMaps;
import oap.util.Result;
import oap.util.Sets;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static oap.testng.Asserts.pathOfResource;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Ignore
public class AtomicUpdateTest extends Fixtures {

    public static final String TABLE_NAME = "atomicUpdateTest";
    public static final String ID_COLUMN_NAME = "id";
    private final AbstractDynamodbFixture fixture = new TestContainerDynamodbFixture();

    public AtomicUpdateTest() {
        fixture( fixture );
        var kernel = new Kernel( Module.CONFIGURATION.urlsFromClassPath() );
        kernel.start( pathOfResource( getClass(), "/oap/dynamodb/test-application.conf" ) );
    }

    @BeforeMethod
    public void beforeMethod() {
        System.setProperty( "TMP_PATH", TestDirectoryFixture.testDirectory().toAbsolutePath().toString().replace( '\\', '/' ) );
    }

    @Test
    public void atomicUpdateShouldInitializeField() throws IOException {
        var client = fixture.getDynamodbClient();

        client.start();
        client.waitConnectionEstablished();
        client.deleteTableIfExists( TABLE_NAME );
        client.createTableIfNotExist( TABLE_NAME, ID_COLUMN_NAME );
        Key key = new Key( TABLE_NAME, ID_COLUMN_NAME, "IFA" );

        client.updateRecordAtomic(
                key,
                Collections.singletonMap( "vvvv", AttributeValue.fromS( "vvv" ) ),
                null,
                -1 );

        Result<Map<String, AttributeValue>, DynamodbClient.State> result = client.getRecord( key, null );
        assertThat( result.isSuccess() ).isTrue();
        //first update is 0 + 1 = 1
        assertThat( result.getSuccessValue().get( "generation" ).n() ).isEqualTo( "1" );
    }

    @Test
    public void atomicUpdateShouldIncrementItsField() throws IOException {
        var client = fixture.getDynamodbClient();

        client.start();
        client.waitConnectionEstablished();
        client.deleteTableIfExists( TABLE_NAME );
        client.createTableIfNotExist( TABLE_NAME, ID_COLUMN_NAME );
        Key key = new Key( TABLE_NAME, ID_COLUMN_NAME, "IFA" );

        client.updateRecordAtomic(
                key,
                Collections.singletonMap( "vvvv", AttributeValue.fromS( "vvv" ) ),
                null,
                20 );

        Result<Map<String, AttributeValue>, DynamodbClient.State> result = client.getRecord( key, null );
        assertThat( result.getSuccessValue().get( "generation" ).n() ).isEqualTo( "1" );

        client.updateRecordAtomic(
                key,
                Collections.singletonMap( "vvvv", AttributeValue.fromS( "vvv" ) ),
                null,
                1 );

        result = client.getRecord( key, null );
        assertThat( result.getSuccessValue().get( "generation" ).n() ).isEqualTo( "2" );
    }

    @Test
    public void atomicUpdateShouldSkipUpdateIfVersionDoesNotFit() throws IOException {
        var client = fixture.getDynamodbClient();

        client.start();
        client.waitConnectionEstablished();
        client.deleteTableIfExists( TABLE_NAME );
        client.createTableIfNotExist( TABLE_NAME, ID_COLUMN_NAME );
        Key key = new Key( TABLE_NAME, ID_COLUMN_NAME, "IFA" );

        assertThat( client.updateRecordAtomic(
                key,
                HashMaps.of(
                        "vvvv", AttributeValue.fromS( "v1" )
                ),
                null,
                39 ).isSuccess() ).isTrue(); //any number for first update

        Result<Map<String, AttributeValue>, DynamodbClient.State> result = client.getRecord( key, null );
        assertThat( result.getSuccessValue().get( "generation" ).n() ).isEqualTo( "1" );

        Result<UpdateItemResponse, DynamodbClient.State> resultOfInvalidOperation = client.updateRecordAtomic(
                key,
                HashMaps.of(
                        "vvvv", AttributeValue.fromS( "v2" )
                ),
                null,
                18 ); //number is not equal to actual 19, so update should not happen
        assertThat( resultOfInvalidOperation.isSuccess() ).isFalse();
        assertThat( resultOfInvalidOperation.getFailureValue() ).isEqualTo( DynamodbClient.State.VERSION_CHECK_FAILED );

        result = client.getRecord( key, null );
        assertThat( result.getSuccessValue().get( "generation" ).n() ).isEqualTo( "1" );
        assertThat( result.getSuccessValue().get( "vvvv" ).s() ).isEqualTo( "v1" );
    }

    @Test
    public void atomicUpdatesShouldRetryIfConcurrent() throws Exception {
        var client = fixture.getDynamodbClient();

        client.start();
        client.waitConnectionEstablished();
        client.deleteTableIfExists( TABLE_NAME );
        client.createTableIfNotExist( TABLE_NAME, ID_COLUMN_NAME );
        Key key = new Key( TABLE_NAME, ID_COLUMN_NAME, "IFA" );
        AtomicInteger counter = new AtomicInteger( 0 );

        ExecutorService service = Executors.newFixedThreadPool( 20 );
        for ( int i = 0; i < 1000; i++ ) {
            service.submit( () -> {
                Result<UpdateItemResponse, DynamodbClient.State> result = client.updateRecordAtomicWithRetry(
                        key,
                        Sets.of( "version", "id" ),
                        valueMap -> {
                            Map<String, AttributeValue> res = new HashMap<>( valueMap );
                            res.put( "version", AttributeValue.fromS( "v" + counter.get() ) );
                            return res;
                        },
                        1000,
                        counter.get() );
                if ( result.isSuccess() ) counter.incrementAndGet();
                if ( counter.get() % 5 == 0 || !result.isSuccess() ) System.err.println( "#" + counter.get() + " -> " + result.isSuccess() );
                Result<Map<String, AttributeValue>, DynamodbClient.State> record = client.getRecord( key, null );
            } );
        }
        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );

        Result<Map<String, AttributeValue>, DynamodbClient.State> result = client.getRecord( key, null );
        assertThat( result.getSuccessValue().get( "generation" ).n() ).isEqualTo( "" + counter.get() );

        client.updateRecordAtomic(
                key,
                Collections.singletonMap( "version", AttributeValue.fromS( "final" ) ),
                null,
                counter.get() );

        result = client.getRecord( key, null );
        assertThat( result.getSuccessValue().get( "generation" ).n() ).isGreaterThanOrEqualTo( "" + ( counter.get() ) );
    }
}
