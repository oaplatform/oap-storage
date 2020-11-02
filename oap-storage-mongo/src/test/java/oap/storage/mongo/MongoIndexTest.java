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

import oap.testng.Fixtures;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.List.of;
import static oap.storage.mongo.MongoIndex.IndexInfo.Direction.ASC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

/**
 * Created by igor.petrenko on 2020-11-02.
 */
public class MongoIndexTest extends Fixtures {
    public MongoIndexTest() {
        fixture( new MongoFixture() );
    }

    @Test
    public void testUpdate_CreateNewIndex() {
        try( MongoClient client = new MongoClient( MongoFixture.mongoHost, MongoFixture.mongoPort, "testdb", MongoFixture.mongoDatabase ) ) {
            var collection = client.getCollection( "test" );
            var mongoIndex = new MongoIndex( collection );

            mongoIndex.update( "idx1", of( "a" ), true );
            mongoIndex.update( "idx1", of( "a", "b" ), true );
            mongoIndex.update( "idx1", of( "a", "b" ), true );
            mongoIndex.update( "idx1", of( "a", "b" ), false );

            var info = mongoIndex.getInfo( "idx1" );
            assertNotNull( info );
            assertFalse( info.unique );
            assertThat( info.keys ).containsExactlyEntriesOf( Map.of( "a", ASC, "b", ASC ) );
        }
    }
}
