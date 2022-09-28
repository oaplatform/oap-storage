package oap.dynamodb;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import org.testcontainers.containers.GenericContainer;
import java.net.URI;

@Slf4j
public class TestContainerDynamodbFixture extends AbstractDynamodbFixture {
    private GenericContainer genericContainer;
    protected URI uri;
    protected StaticCredentialsProvider provider;
    @Override
    protected DynamodbClient createClient() {
        uri = URI.create( "http://localhost:" + genericContainer.getFirstMappedPort() );
        provider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create( AbstractDynamodbFixture.AWS_ACCESS_KEY_ID, AbstractDynamodbFixture.AWS_SECRET_ACCESS_KEY )
        );
        DynamoDbClient dynamoDbAsyncClient = DynamoDbClient.builder()
                .region( Region.US_EAST_1 )
                .endpointOverride( uri )
                .credentialsProvider( provider )
                .build();

        DynamodbClient dynamodbClient = new DynamodbClient( dynamoDbAsyncClient );
        dynamodbClient.setStreamClient( dynamodbClient.createStreamClient( uri, provider, Region.US_EAST_1 ) );
        return dynamodbClient;
    }

    @BeforeClass
    public void beforeClass() {
        genericContainer =
                new GenericContainer( DockerImageName
                        .parse( "amazon/dynamodb-local" ) )
                        .withCommand( "-jar DynamoDBLocal.jar -inMemory -sharedDb" )
                        .withExposedPorts( 8000 );
        genericContainer.start();
        log.info( "Container {} started, listening to {}", genericContainer.getContainerId(), genericContainer.getFirstMappedPort() );

    }

    @AfterClass
    public void tearDown() {
        genericContainer.stop();
    }
}
