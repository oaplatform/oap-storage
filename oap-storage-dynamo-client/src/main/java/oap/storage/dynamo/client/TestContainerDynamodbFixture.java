/*
 * The MIT License (MIT)
 *
 * Copyright (c) Open Application Platform Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package oap.storage.dynamo.client;

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
                AwsBasicCredentials.create( AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY )
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