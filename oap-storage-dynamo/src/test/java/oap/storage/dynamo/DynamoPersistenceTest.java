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

package oap.storage.dynamo;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import oap.dynamodb.DynamodbClient;
import oap.dynamodb.DynamodbFixture;
import oap.dynamodb.Key;
import oap.dynamodb.batch.WriteBatchOperationHelper;
import oap.dynamodb.crud.CreateItemOperation;
import oap.id.Identifier;
import oap.storage.DynamoPersistence;
import oap.storage.MemoryStorage;
import oap.storage.Metadata;
import oap.system.Env;
import oap.testng.Fixtures;
import oap.testng.TestDirectoryFixture;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static oap.storage.Storage.Lock.SERIALIZED;
import static oap.testng.Asserts.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@Ignore
public class DynamoPersistenceTest extends Fixtures {

    private final DynamodbFixture fixture;

    private final Identifier<String, Bean> beanIdentifier =
        Identifier.<Bean>forId( o -> o.id, ( o, id ) -> o.id = id )
            .suggestion( o -> o.name )
            .build();

    public DynamoPersistenceTest() {
        Env.set( "AWS_ACCESS_KEY_ID", "dummy" );
        Env.set( "AWS_SECRET_ACCESS_KEY", "dummy" );
        fixture( fixture = new DynamodbFixture() );
        fixture( TestDirectoryFixture.FIXTURE );
    }

    private Function<Map<String, AttributeValue>, Metadata<Bean>> fromDynamo = map -> {
        final Metadata<Bean> metadata = new Metadata<>() {}; //todo discuss metadata creation (override modifiedWhen...)
        metadata.object = new Bean( map.get( "id" ).s(), map.get( "firstName" ).s() );
        return metadata;
    };

    private Function<Metadata<Bean>, Map<String, Object>> toDynamo = metadata -> {
        final HashMap<String, Object> objectHashMap = new HashMap<>();
        objectHashMap.put( "firstName", metadata.object.name );
        return objectHashMap;
    };

    @Test
    public void load() throws IOException {
        var storage = new MemoryStorage<>( beanIdentifier, SERIALIZED );

        try( var dynamodbClient = new DynamodbClient( DynamodbFixture.DYNAMODB_PROTOCOL, DynamodbFixture.DYNAMODB_HOSTS, DynamodbFixture.DYNAMODB_PORT, DynamodbFixture.AWS_ACCESS_KEY_ID, DynamodbFixture.AWS_SECRET_ACCESS_KEY, DynamodbFixture.AWS_REGION );
             var persistence = new DynamoPersistence<>( dynamodbClient, "test", 6000, storage, fromDynamo, toDynamo ) ) {
            dynamodbClient.waitConnectionEstablished();
            dynamodbClient.createTable( "test", 2, 1, "id", "S", null, null, null );
            final WriteBatchOperationHelper batchWriter = new WriteBatchOperationHelper( dynamodbClient );
            batchWriter.addOperation( new CreateItemOperation( new Key( "test", "id", "1" ), ImmutableMap.of( "firstName", "John" ) ) );
            batchWriter.write();

            persistence.watch = false;
            persistence.preStart();

            assertThat( storage ).containsExactly( new Bean( "1", "John" ) );
        }
    }

    @Test
    public void watch() throws IOException {
        var storage = new MemoryStorage<>( beanIdentifier, SERIALIZED );

        try( var dynamodbClient = new DynamodbClient( DynamodbFixture.DYNAMODB_PROTOCOL, DynamodbFixture.DYNAMODB_HOSTS, DynamodbFixture.DYNAMODB_PORT, DynamodbFixture.AWS_ACCESS_KEY_ID, DynamodbFixture.AWS_SECRET_ACCESS_KEY, DynamodbFixture.AWS_REGION ) ) {
            dynamodbClient.waitConnectionEstablished();
            var persistence = new DynamoPersistence<>( dynamodbClient, "test", 6000, storage, fromDynamo, toDynamo );
            dynamodbClient.createTable( "test", 2, 1, "id", "S", null, null, builder -> builder.streamSpecification( StreamSpecification.builder().streamEnabled( true ).streamViewType( StreamViewType.NEW_AND_OLD_IMAGES ).build() ) );

            persistence.watch = true;
            persistence.preStart();
            dynamodbClient.update( new Key( "test", "id", "1" ), "firstName", "John" );
            assertEventually( 500, 10, () -> assertThat( storage ).containsExactly( new Bean( "1", "John" ) ) );

            dynamodbClient.update( new Key( "test", "id", "1" ), "firstName", "Ann" );
            assertEventually( 500, 10, () -> assertThat( storage ).containsExactly( new Bean( "1", "Ann" ) ) );

        }
    }

    @Test
    public void sync() throws IOException {
        var storage = new MemoryStorage<>( beanIdentifier, SERIALIZED );

        try( var dynamodbClient = new DynamodbClient( DynamodbFixture.DYNAMODB_PROTOCOL, DynamodbFixture.DYNAMODB_HOSTS, DynamodbFixture.DYNAMODB_PORT, DynamodbFixture.AWS_ACCESS_KEY_ID, DynamodbFixture.AWS_SECRET_ACCESS_KEY, DynamodbFixture.AWS_REGION ) ) {
            dynamodbClient.waitConnectionEstablished();
            var persistence = new DynamoPersistence<>( dynamodbClient, "test", 500, storage, fromDynamo, toDynamo );
            dynamodbClient.createTable( "test", 2, 1, "id", "S", null, null, builder -> builder.streamSpecification( StreamSpecification.builder().streamEnabled( true ).streamViewType( StreamViewType.NEW_AND_OLD_IMAGES ).build() ) );
            dynamodbClient.update( new Key( "test", "id", "1" ), "firstName", "John" );
            persistence.preStart();

            storage.store( new Bean( "2", "AnnaStore" ) );
            storage.store( new Bean( "1", "JohnUpdated" ) );

            final List<Map<String, AttributeValue>> mapList = getMapList( storage );

            assertEventually( 500, 10, () ->
                assertThat( dynamodbClient.getRecord( "test", 10, "id", null ).getRecords() )
                    .containsExactly( mapList.get( 0 ), mapList.get( 1 ) ) );

        }
    }

    @Test
    public void syncWithDeletedItems() throws IOException {
        var storage = new MemoryStorage<>( beanIdentifier, SERIALIZED );

        try( var dynamodbClient = new DynamodbClient( DynamodbFixture.DYNAMODB_PROTOCOL, DynamodbFixture.DYNAMODB_HOSTS, DynamodbFixture.DYNAMODB_PORT, DynamodbFixture.AWS_ACCESS_KEY_ID, DynamodbFixture.AWS_SECRET_ACCESS_KEY, DynamodbFixture.AWS_REGION ) ) {
            dynamodbClient.waitConnectionEstablished();
            var persistence = new DynamoPersistence<>( dynamodbClient, "test", 500, storage, fromDynamo, toDynamo );
            dynamodbClient.createTable( "test", 2, 1, "id", "S", null, null, builder -> builder.streamSpecification( StreamSpecification.builder().streamEnabled( true ).streamViewType( StreamViewType.NEW_AND_OLD_IMAGES ).build() ) );
            dynamodbClient.update( new Key( "test", "id", "1" ), "name", "John" );
            persistence.preStart();

            storage.store( new Bean( "2", "AnnaStore" ) );
            storage.delete( "1" );

            final List<Map<String, AttributeValue>> mapList = getMapList( storage );

            assertEventually( 500, 10, () ->
                assertThat( dynamodbClient.getRecord( "test", 10, "id", null ).getRecords() )
                    .containsExactly( mapList.get( 0 ) ) );
        }
    }

    @Test
    public void bothStoragesShouldBeEmpty() throws IOException {
        var storage = new MemoryStorage<>( beanIdentifier, SERIALIZED );

        try( var dynamodbClient = new DynamodbClient( DynamodbFixture.DYNAMODB_PROTOCOL, DynamodbFixture.DYNAMODB_HOSTS, DynamodbFixture.DYNAMODB_PORT, DynamodbFixture.AWS_ACCESS_KEY_ID, DynamodbFixture.AWS_SECRET_ACCESS_KEY, DynamodbFixture.AWS_REGION ) ) {
            dynamodbClient.waitConnectionEstablished();
            var persistence = new DynamoPersistence<>( dynamodbClient, "test", 500, storage, fromDynamo, toDynamo );
            dynamodbClient.createTable( "test", 2, 1, "id", "S", null, null, builder -> builder.streamSpecification( StreamSpecification.builder().streamEnabled( true ).streamViewType( StreamViewType.NEW_AND_OLD_IMAGES ).build() ) );
            dynamodbClient.update( new Key( "test", "id", "1" ), "firstName", "John" );
            dynamodbClient.update( new Key( "test", "id", "2" ), "firstName", "Anna" );
            persistence.watch = true;
            persistence.preStart();
            dynamodbClient.delete( new Key( "test", "id", "1" ), null );
            assertEventually( 500, 10, () -> assertThat( storage ).containsExactly( new Bean( "2", "Anna" ) ) );
            assertEventually( 500, 10, () -> assertThat( dynamodbClient.getRecord( "test", 10, "id", null ).getRecords() )
                .containsExactly( ImmutableMap.of( "id", AttributeValue.builder().s( "2" ).build(), "firstName", AttributeValue.builder().s( "Anna" ).build() ) ) );
            storage.delete( "2" );

            assertEventually( 500, 10, () -> assertThat( storage.size() ).isEqualTo( 0 ) );
            assertEventually( 500, 10, () -> assertThat( dynamodbClient.getRecord( "test", 10, "id", null ).getRecords().size() ).isEqualTo( 0 ) );
        }
    }

    private List<Map<String, AttributeValue>> getMapList( MemoryStorage<String, Bean> storage ) {
        return storage.list().stream()
            .map( bean -> ImmutableMap.of( "id", AttributeValue.builder().s( bean.id ).build(),
                "firstName", AttributeValue.builder().s( bean.name ).build() ) ).collect( Collectors.toList() );
    }
}
