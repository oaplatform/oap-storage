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

package oap.storage.mongo.memory;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import lombok.extern.slf4j.Slf4j;
import oap.storage.mongo.MongoClient;
import oap.storage.mongo.Version;
import oap.testng.Env;
import oap.testng.EnvFixture;
import oap.testng.Suite;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import static oap.testng.Asserts.contentOfTestResource;

@Slf4j
public class MongoFixture extends EnvFixture {
    public final int port;
    public final String database;
    private MongoClient mongoClient;
    private MongoServer server;

    public MongoFixture() {
        define( "MONGO_PORT", port = Env.port( "MONGO_PORT" ) );
        define( "MONGO_DATABASE", database = "db_" + StringUtils.replaceChars( Suite.uniqueExecutionId(), ".-", "_" ) );
    }

    @Override
    public void beforeMethod() {
        super.beforeMethod();
        this.server = new MongoServer( new MemoryBackend() );
        log.info( "mongo port = {}", port );
        this.server.bind( "localhost", port );
        this.mongoClient = new MongoClient( "localhost", port, database, database );
    }

    @Override
    public void afterMethod() {
        this.mongoClient.close();
        this.server.shutdownNow();
        super.afterMethod();
    }

    public void insertDocument( Class<?> contextClass, String collection, String resourceName ) {
        this.mongoClient.getCollection( collection ).insertOne( Document.parse( contentOfTestResource( contextClass, resourceName ) ) );
    }

    public void initializeVersion( Version version ) {
        this.mongoClient.updateVersion( version );
    }

    public MongoClient client() {
        return mongoClient;
    }
}
