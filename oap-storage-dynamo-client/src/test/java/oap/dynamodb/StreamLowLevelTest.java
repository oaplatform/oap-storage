package oap.dynamodb;

import lombok.extern.slf4j.Slf4j;
import oap.application.Kernel;
import oap.application.module.Module;
import oap.dynamodb.streams.DynamodbStreamsRecordProcessor;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static oap.testng.Asserts.pathOfResource;
import static org.assertj.core.api.Assertions.assertThat;

@Ignore
@Slf4j
public class StreamLowLevelTest extends Fixtures {
    private final String tableName = "tableForStream";
    private final String keyName = "id";
    private final String longId = "787846fd-6e98-4ca9-a2d4-236ff93aa027";

    private final AbstractDynamodbFixture fixture = new TestContainerDynamodbFixture();

    public StreamLowLevelTest() {
        fixture( fixture );
        Kernel kernel = new Kernel( Module.CONFIGURATION.urlsFromClassPath() );
        kernel.start( pathOfResource( getClass(), "/oap/dynamodb/test-application.conf" ) );
    }

    @BeforeMethod
    public void beforeMethod() {
        System.setProperty( "TMP_PATH", TestDirectoryFixture.testDirectory().toAbsolutePath().toString().replace( '\\', '/' ) );
    }

    @Test
    public void testClient() throws Exception {
        var client = fixture.getDynamodbClient();
        client.start();
        client.waitConnectionEstablished();
        client.deleteTable( tableName );

        client.createTable( tableName, 2, 1, keyName, "S", null, null,
                z -> z.streamSpecification( StreamSpecification.builder()
                        .streamEnabled( true )
                        .streamViewType( StreamViewType.NEW_AND_OLD_IMAGES )
                        .build() ) );

        //create stream for a table
        TableDescription table = client.describeTable( tableName, null );
        String streamArn = table.latestStreamArn();
        assertThat( streamArn ).isNotEmpty();
        log.info( "Current stream ARN for " + tableName + ": " + streamArn );

        CountDownLatch recordsAreWritten = new CountDownLatch( 1 );
        ExecutorService service = Executors.newFixedThreadPool( 2 );
        service.submit( () -> {
            // do some modifications
            Key key = createRandomKey();
            client.update( key, "v1", false ); // update 1
            client.update( key, "v1", true ); // update 2
            client.update( key, "v2", Math.PI ); // update 3
            client.update( key, "v2", null ); // update 4
            client.delete( key, null ); // delete
            recordsAreWritten.countDown();
        } );
        List<String> changes = new ArrayList<>();
        service.submit( () -> {
            DynamodbStreamsRecordProcessor processor = DynamodbStreamsRecordProcessor.builder( client ).build();
            try {
                recordsAreWritten.await( 5, TimeUnit.SECONDS );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }
            extractChanges( streamArn, processor, changes );
        } );
        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );

        assertThat( changes.toString() ).isEqualTo( "["
                        + "id:{id=AttributeValue(S=787846fd-6e98-4ca9-a2d4-236ff93aa0271)}|v1:null->false|v2:null->null, " // update 1
                        + "id:{id=AttributeValue(S=787846fd-6e98-4ca9-a2d4-236ff93aa0271)}|v1:false->true|v2:null->null, " // update 2
                        + "id:{id=AttributeValue(S=787846fd-6e98-4ca9-a2d4-236ff93aa0271)}|v1:true->true|v2:null->3.141592653589793, " // update 3
                        + "id:{id=AttributeValue(S=787846fd-6e98-4ca9-a2d4-236ff93aa0271)}|v1:true->true|v2:3.141592653589793->null, " // update 4
                        + "id:{id=AttributeValue(S=787846fd-6e98-4ca9-a2d4-236ff93aa0271)}|v1:true->null|v2:null->null" // delete
                        + "]" );

        client.deleteTableIfExists( tableName );
    }

    @Test
    public void testClientWithRecreate() throws Exception {
        var client = fixture.getDynamodbClient();
        client.start();
        client.waitConnectionEstablished();
        client.deleteTable( tableName );

        client.createTable( tableName, 2, 1, keyName, "S", null, null,
                z -> z.streamSpecification( StreamSpecification.builder()
                        .streamEnabled( true )
                        .streamViewType( StreamViewType.NEW_AND_OLD_IMAGES )
                        .build() ) );
        client.recreateTable( tableName, keyName );

        //create stream for a table
        TableDescription table = client.describeTable( tableName, null );
        String streamArn = table.latestStreamArn();
        assertThat( streamArn ).isNotEmpty();
        log.info( "Current stream ARN for " + tableName + ": " + streamArn );

        CountDownLatch recordsAreWritten = new CountDownLatch( 1 );
        ExecutorService service = Executors.newFixedThreadPool( 2 );
        service.submit( () -> {
            // do some modifications to reveal it in stream
            Key key = createRandomKey();
            client.update( key, "v1", true );
            recordsAreWritten.countDown();
        } );
        List<String> changes = new ArrayList<>();
        service.submit( () -> {
            DynamodbStreamsRecordProcessor processor = DynamodbStreamsRecordProcessor.builder( client ).build();
            try {
                recordsAreWritten.await( 5, TimeUnit.SECONDS );
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }
            extractChanges( streamArn, processor, changes );
        } );
        service.shutdown();
        service.awaitTermination( 1, TimeUnit.MINUTES );

        assertThat( changes ).isNotEmpty();

        client.deleteTableIfExists( tableName );
    }

    private AtomicInteger counter = new AtomicInteger();

    @NotNull
    private Key createRandomKey() {
        return new Key( tableName, keyName, longId + counter.incrementAndGet() );
    }

    private void extractChanges( String streamArn, DynamodbStreamsRecordProcessor processor, List<String> changes ) {
        processor.processRecords( streamArn, record -> {
            AttributeValue newV1Value = record.dynamodb().newImage().get( "v1" );
            if ( newV1Value == null ) newV1Value = AttributeValue.fromBool( null );
            AttributeValue newV2Value = record.dynamodb().newImage().get( "v2" );
            if ( newV2Value == null ) newV2Value = AttributeValue.fromN( null );
            AttributeValue oldV1Value = record.dynamodb().oldImage().get( "v1" );
            if ( oldV1Value == null ) oldV1Value = AttributeValue.fromBool( null );
            AttributeValue oldV2Value = record.dynamodb().oldImage().get( "v2" );
            if ( oldV2Value == null ) oldV2Value = AttributeValue.fromN( null );

            changes.add( "id:" + record.dynamodb().keys()
                    + "|v1:" + oldV1Value.bool() + "->" + newV1Value.bool()
                    + "|v2:" + oldV2Value.n() + "->" + newV2Value.n() );
        } );
    }
}
