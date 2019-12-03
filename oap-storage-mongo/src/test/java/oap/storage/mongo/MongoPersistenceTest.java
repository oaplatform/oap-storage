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

package oap.storage.mongo;

import com.mongodb.client.model.BulkWriteOptions;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.id.Identifier;
import oap.storage.MemoryStorage;
import oap.storage.MongoPersistence;
import oap.testng.Fixtures;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.types.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import static oap.storage.Storage.Lock.SERIALIZED;
import static oap.testng.Asserts.assertEventually;
import static oap.testng.Env.tmpRoot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * It requires installed MongoDB on the machine with enabled Replica Set Oplog
 *
 * @see <a href="https://docs.mongodb.com/manual/administration/install-community/">Install MongoDB Community Edition</a>
 * @see <a href="https://docs.mongodb.com/manual/tutorial/deploy-replica-set-for-testing/">Deploy a Replica Set for
 * Testing and Development</a>
 */
@Slf4j
public class MongoPersistenceTest extends Fixtures {

    protected Identifier<String, Bean> beanIdentifier =
        Identifier.<Bean>forId( o -> o.id, ( o, id ) -> o.id = id )
            .suggestion( o -> o.name )
            .length( 10 )
            .build();
    protected Identifier<String, Bean> beanIdentifierWithoutName =
        Identifier.<Bean>forId( o -> o.id, ( o, id ) -> o.id = id )
            .suggestion( ar -> ObjectId.get().toString() )
            .length( 10 )
            .build();

    {
        fixture( new MongoFixture() );
    }

    @Test
    public void store() {
        var storage1 = new MemoryStorage<>( beanIdentifier, SERIALIZED );
        try( var persistence = new MongoPersistence<>( MongoFixture.mongoClient, "test", 6000, storage1 ) ) {

            persistence.start();
            Bean bean1 = storage1.store( new Bean( "test1" ) );
            Bean bean2 = storage1.store( new Bean( "test2" ) );
            // rewrite bean2 'test2' with 'test3' name
            bean2 = storage1.store( new Bean( bean2.id, "test3" ) );

            log.debug( "bean1 = {}", bean1 );
            log.debug( "bean2 = {}", bean2 );

            assertThat( bean1.id ).isEqualTo( "TST1" );
            assertThat( bean2.id ).isEqualTo( "TST2" );
        }

        // Make sure that for a new connection the objects still present in MongoDB
        var storage2 = new MemoryStorage<>( beanIdentifier, SERIALIZED );
        try( var persistence = new MongoPersistence<>( MongoFixture.mongoClient, "test", 6000, storage2 ) ) {
            persistence.start();
            assertThat( storage2.select() ).containsOnly(
                new Bean( "TST1", "test1" ),
                new Bean( "TST2", "test3" )
            );
            assertThat( persistence.collection.countDocuments() ).isEqualTo( 2 );
        }

    }

    @Test
    public void delete() {
        var storage = new MemoryStorage<>( beanIdentifierWithoutName, SERIALIZED );
        try( var persistence = new MongoPersistence<>( MongoFixture.mongoClient, "test", 50, storage ) ) {
            persistence.start();
            var bean1 = storage.store( new Bean() );
            storage.store( new Bean() );

            storage.delete( bean1.id );
            // one bean is removed, one is left
            assertEventually( 100, 100, () -> assertThat( persistence.collection.countDocuments() ).isEqualTo( 1 ) );
        }
    }

    @Test()
    public void update() {
        var storage1 = new MemoryStorage<>( Identifier.<Bean>forId( o -> o.id, ( o, id ) -> o.id = id )
            .suggestion( o -> o.name )
            .build(), SERIALIZED );
        try( var persistence = new MongoPersistence<>( MongoFixture.mongoClient, "test", 6000, storage1 ) ) {
            persistence.start();
            storage1.store( new Bean( "111", "initialName" ) );
            storage1.update( "111", bean -> {
                bean.name = "newName";
                return bean;
            } );
        }
        var storage2 = new MemoryStorage<>( Identifier.<Bean>forId( o -> o.id, ( o, id ) -> o.id = id )
            .suggestion( o -> o.name )
            .build(), SERIALIZED );
        try( var persistence = new MongoPersistence<>( MongoFixture.mongoClient, "test", 6000, storage2 ) ) {
            persistence.start();
            assertThat( storage2.select() )
                .containsExactly( new Bean( "111", "newName" ) );
        }
    }

    @Test( expectedExceptions = BsonMaximumSizeExceededException.class )
    public void storeWithMongoException() throws Exception {
        var storage1 = new MemoryStorage<>( beanIdentifier, SERIALIZED );
        Exception exception = null;
        try( var persistence = new MongoPersistence<>( MongoFixture.mongoClient, "test", 6000, storage1,
            tmpRoot.toString(), 0 ) ) {

            java.lang.reflect.Field collection = persistence.getClass().getDeclaredField( "collection" );
            collection.setAccessible( true );
            var mock = spy( persistence.collection );
            collection.set( persistence, mock );
            doThrow( new BsonMaximumSizeExceededException( "Payload document size is larger than maximum of 16793600" ) )
                .when( mock ).bulkWrite( anyList(), any( BulkWriteOptions.class ) );

            storage1.store( new Bean( "test1" ) );
            persistence.start();
        } catch( BsonMaximumSizeExceededException e ) {
            exception = e;
            try( var stream = Files.newDirectoryStream( tmpRoot, "*_failed.json" ) ) {
                Iterator<Path> iterator = stream.iterator();
                Assert.assertTrue( iterator.hasNext() );
                while( iterator.hasNext() ) Assert.assertTrue( Files.exists( iterator.next() ) );
            }
        }

        try( var persistence = new MongoPersistence<>( MongoFixture.mongoClient, "test", 6000, storage1,
            tmpRoot.toString(), 0 ) ) {
            persistence.start();
        }

        try( var stream = Files.newDirectoryStream( tmpRoot, "*_failed.json" ) ) {
            Assert.assertFalse( stream.iterator().hasNext() );
        }
        if( exception != null ) throw exception;
    }

    @ToString
    @EqualsAndHashCode
    public static class Bean {
        public String id;
        public String name;
        public int c;

        Bean( String id, String name ) {
            this( name );
            this.id = id;
        }

        Bean( String name ) {
            this.name = name;
        }

        Bean() {
        }
    }
}
