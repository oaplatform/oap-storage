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

import com.mongodb.client.MongoCollection;
import oap.testng.Fixtures;
import org.bson.Document;
import org.testng.annotations.Test;

import static oap.testng.Asserts.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deploy a Replica Set in order this test passes
 *
 * @see <a href=https://docs.mongodb.com/manual/tutorial/deploy-replica-set/>Deploy a Replica Set for
 * Testing and Development</a>
 */
public class OplogServiceTest extends Fixtures {
    private final MongoFixture mongoFixture;

    public OplogServiceTest() {
        fixture( this.mongoFixture = new MongoFixture() );
    }

    @Test
    public void oplog() {
        try( var mongoClient = new MongoClient( mongoFixture.mongoHost, mongoFixture.mongoPort, mongoFixture.mongoDatabase );
             var oplogService = new OplogService( mongoClient ) ) {

            mongoClient.start();

            var listener = new StringBuilder();

            String collection = "test_OplogServiceTest";
            oplogService.addOplogListener( new OplogService.OplogListener() {
                @Override
                public void updated( String mongoId ) {
                    listener.append( 'u' );
                }

                @Override
                public void deleted( String mongoId ) {
                    listener.append( 'd' );
                }

                @Override
                public void inserted( String mongoId ) {
                    listener.append( 'i' );
                }

                @Override
                public String collectionName() {
                    return collection;
                }
            } );

            oplogService.start();

            MongoCollection<Document> mongoCollection = mongoClient.getCollection( collection );
            mongoCollection.insertOne( new Document( "test", "test" ) );
            mongoClient.getCollection( "test_OplogServiceTest2" ).updateOne( new Document( "test", "test" ), new Document( "$set", new Document( "test", "test2" ) ) );
            mongoCollection.updateOne( new Document( "test", "test" ), new Document( "$set", new Document( "test", "test2" ) ) );
            mongoCollection.updateOne( new Document( "test", "test2" ), new Document( "$set", new Document( "test", "test3" ) ) );
            mongoCollection.deleteOne( new Document( "test", "test3" ) );

            assertEventually( 100, 100, () -> assertThat( listener.toString() ).isEqualTo( "iuud" ) );
        }
    }
}
