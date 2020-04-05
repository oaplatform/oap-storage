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

package oap.storage;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.id.Identifier;
import oap.io.Files;
import oap.storage.mongo.MongoClient;
import oap.storage.mongo.MongoFixture;
import oap.testng.Env;
import oap.testng.Fixtures;
import oap.testng.TestDirectory;
import org.bson.types.ObjectId;
import org.testng.annotations.Test;

import java.nio.file.Path;

import static oap.storage.Storage.Lock.SERIALIZED;
import static oap.testng.Asserts.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * It requires installed MongoDB on the machine with enabled Replica Set Oplog
 *
 * @see <a href="https://docs.mongodb.com/manual/administration/install-community/">Install MongoDB Community Edition</a>
 * @see <a href="https://docs.mongodb.com/manual/tutorial/deploy-replica-set-for-testing/">Deploy a Replica Set for
 * Testing and Development</a>
 */
@Slf4j
public class MongoPersistenceTest extends Fixtures {

    private final MongoFixture mongoFixture;
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

    public MongoPersistenceTest() {
        fixture( mongoFixture = new MongoFixture() );
        fixture( TestDirectory.FIXTURE );
    }

    @Test
    public void store() {
        var storage1 = new MemoryStorage<>( beanIdentifier, SERIALIZED );
        try( var mongoClient = new MongoClient( mongoFixture.mongoHost, mongoFixture.mongoPort, mongoFixture.mongoDatabase );
             var persistence = new MongoPersistence<>( mongoClient, "test", 6000, storage1 ) ) {
            mongoClient.start();
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
        try( var mongoClient = new MongoClient( mongoFixture.mongoHost, mongoFixture.mongoPort, mongoFixture.mongoDatabase );
             var persistence = new MongoPersistence<>( mongoClient, "test", 6000, storage2 ) ) {
            mongoClient.start();
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
        try( var mongoClient = new MongoClient( mongoFixture.mongoHost, mongoFixture.mongoPort, mongoFixture.mongoDatabase );
             var persistence = new MongoPersistence<>( mongoClient, "test", 50, storage ) ) {
            mongoClient.start();
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
        try( var mongoClient = new MongoClient( mongoFixture.mongoHost, mongoFixture.mongoPort, mongoFixture.mongoDatabase );
             var persistence = new MongoPersistence<>( mongoClient, "test", 6000, storage1 ) ) {
            mongoClient.start();
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
        try( var mongoClient = new MongoClient( mongoFixture.mongoHost, mongoFixture.mongoPort, mongoFixture.mongoDatabase );
             var persistence = new MongoPersistence<>( mongoClient, "test", 6000, storage2 ) ) {
            mongoClient.start();
            persistence.start();
            assertThat( storage2.select() )
                .containsExactly( new Bean( "111", "newName" ) );
        }
    }

    @Test
    public void storeTooBig() {
        var storage = new MemoryStorage<>( beanIdentifier, SERIALIZED );
        Path crashDumpPath = Env.tmpPath( "failures" );
        String table = "test";
        try( var mongoClient = new MongoClient( mongoFixture.mongoHost, mongoFixture.mongoPort, mongoFixture.mongoDatabase );
             var persistence = new MongoPersistence<>( mongoClient, table, 6000, storage, crashDumpPath ) ) {
            mongoClient.start();
            persistence.start();
            storage.store( new Bean( "X".repeat( 16793600 + 1 ) ) );
        }
        assertThat( Files.wildcard( crashDumpPath.resolve( table ), "*.json.gz" ) ).hasSize( 1 );
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
