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

import lombok.extern.slf4j.Slf4j;
import oap.testng.Env;
import oap.testng.Fixture;
import oap.testng.Teamcity;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.joda.time.DateTimeUtils;

import java.util.Date;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static oap.testng.Asserts.contentOfTestResource;

/**
 * Created by igor.petrenko on 09/20/2019.
 */
@Slf4j
public class MongoFixture implements Fixture {
    public final int mongoPort;
    public final String mongoHost;
    public final String mongoDatabase;

    private MongoClient mongoClient;

    public MongoFixture() {
        mongoPort = 27017;
        mongoHost = Env.getEnvOrDefault( "MONGO_HOST", "localhost" );
        mongoDatabase = "db" + StringUtils.replaceChars( Teamcity.buildPrefix(), ".-", "_" ) + "_" + DateTimeUtils.currentTimeMillis();

        System.setProperty( "MONGO_HOST", mongoHost );
        System.setProperty( "MONGO_PORT", String.valueOf( mongoPort ) );
        System.setProperty( "MONGO_DATABASE", mongoDatabase );
    }

    public void dropTestDatabases() {
        final Pattern pattern = Pattern.compile( ".+_(\\d+)" );
        try( com.mongodb.MongoClient mongoClient = new com.mongodb.MongoClient( mongoHost, mongoPort ) ) {
            Consumer<String> drop = database -> {
                Matcher matcher = pattern.matcher( database );
                if( matcher.find() )
                    if( new Date().getTime() - Long.parseLong( matcher.group( 1 ) ) > 1000 * 60 * 60 * 12 )
                        mongoClient.dropDatabase( database );
            };
            mongoClient.listDatabaseNames().forEach( drop );
        }
    }

    public <T> void insertDocument( Class<?> contextClass, String collection, String resourceName ) {
        mongoClient.getCollection( collection ).insertOne( Document.parse( contentOfTestResource( contextClass, resourceName ) ) );
    }

    @Override
    public void beforeMethod() {
        mongoClient = new MongoClient( mongoHost, mongoPort, mongoDatabase, mongoDatabase );
    }

    @Override
    public void afterMethod() {
        mongoClient.dropDatabase();
        mongoClient.close();
    }

    public void initializeVersion( Version version ) {
        mongoClient.updateVersion( version );
    }
}
