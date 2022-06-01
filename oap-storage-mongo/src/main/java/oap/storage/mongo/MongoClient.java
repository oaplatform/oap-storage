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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.connection.ServerDescription;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.util.Lists;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static oap.storage.mongo.MigrationConfig.CONFIGURATION;
import static oap.util.function.Functions.illegalArgument;
import static org.apache.commons.lang.StringUtils.isNotEmpty;

@Slf4j
@ToString( exclude = { "migrations", "shell", "mongoClient", "database" } )
public class MongoClient implements Closeable {
    public final String host;
    public final int port;
    public final String databaseName;
    public final String physicalDatabase;
    protected final MongoShell shell;
    final com.mongodb.client.MongoClient mongoClient;
    private final MongoDatabase database;
    private final List<MigrationConfig> migrations;

    public MongoClient( String host, int port, String database ) {
        this( host, port, database, new MongoShell() );
    }

    public MongoClient( String host, int port, String database, String user, String password ) {
        this( host, port, database, new MongoShell(), user, password );
    }

    public MongoClient( String host, int port, String database, MongoShell shell ) {
        this( host, port, database, database, shell );
    }

    public MongoClient( String host, int port, String database, MongoShell shell, String user, String password ) {
        this( host, port, database, database, shell, user, password );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, MongoShell shell ) {
        this( host, port, database, physicalDatabase, CONFIGURATION.fromClassPath(), shell );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, MongoShell shell, String user, String password ) {
        this( host, port, database, physicalDatabase, CONFIGURATION.fromClassPath(), shell, user, password );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase ) {
        this( host, port, database, physicalDatabase, new MongoShell() );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, String user, String password ) {
        this( host, port, database, physicalDatabase, new MongoShell(), user, password );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, boolean withMigrations ) {
        this( host, port, database, physicalDatabase,
            withMigrations ? CONFIGURATION.fromClassPath() : List.of(), null, null );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, boolean withMigrations, String user, String password ) {
        this( host, port, database, physicalDatabase,
            withMigrations ? CONFIGURATION.fromClassPath() : List.of(), user, password );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, List<MigrationConfig> migrations ) {
        this( host, port, database, physicalDatabase, migrations, new MongoShell() );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, List<MigrationConfig> migrations, String user, String password ) {
        this( host, port, database, physicalDatabase, migrations, new MongoShell(), user, password );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, List<MigrationConfig> migrations, MongoShell shell ) {
        this( host, port, database, physicalDatabase, migrations, shell, null, null );
    }

    public MongoClient( String host, int port, String database, String physicalDatabase, List<MigrationConfig> migrations, MongoShell shell, String user, String password ) {
        this.host = host;
        this.port = port;
        this.databaseName = database;
        this.physicalDatabase = physicalDatabase;
        this.migrations = migrations;
        this.shell = shell;
        final MongoClientSettings.Builder settingsBuilder = defaultBuilder()
            .applyToClusterSettings( b -> b.hosts( Lists.of( new ServerAddress( host, port ) ) ) );
        if( isNotEmpty( user ) && isNotEmpty( password ) ) {
            settingsBuilder.credential( MongoCredential.createCredential( user, database, password.toCharArray() ) );
        }
        this.mongoClient = MongoClients.create( settingsBuilder.build() );
        this.database = mongoClient.getDatabase( physicalDatabase );
        log.debug( "creating mongo client host:{}, port:{}, database:{}, physicalDatabase:{}, migrations:{}, shell:{}, user:{}, password:{}",
            this.host, this.port, this.database, this.physicalDatabase, this.migrations, this.shell, user, password );
    }

    private MongoClientSettings.Builder defaultBuilder() {
        return MongoClientSettings.builder()
            .codecRegistry( CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs( new JodaTimeCodec() ),
                MongoClientSettings.getDefaultCodecRegistry() ) );
    }

    public MongoClient( String uri, String databaseName ) {
        ConnectionString connectionString = new ConnectionString( uri );
        this.mongoClient = MongoClients.create( defaultBuilder()
            .applyConnectionString( connectionString ).build() );
        this.databaseName = databaseName;
        this.physicalDatabase = requireNonNull( connectionString.getDatabase(), "no database specified in " + uri );
        this.database = this.mongoClient.getDatabase( physicalDatabase );
        ServerAddress address = Lists.headOf( this.mongoClient.getClusterDescription().getServerDescriptions() )
            .map( ServerDescription::getAddress )
            .orElseThrow( illegalArgument( "no server description found for " + uri ) );
        this.host = address.getHost();
        this.port = address.getPort();
        this.migrations = CONFIGURATION.fromClassPath();
        this.shell = new MongoShell();
        log.debug( "creating mongo client host:{}, port:{}, database:{}, physicalDatabase:{}, migrations:{}, shell:{}",
            this.host, this.port, this.database, this.physicalDatabase, this.migrations, this.shell );
    }

    public Version databaseVersion() {
        var collection = this.getCollection( "version" );
        var document = collection.find().first();
        return document != null ? new Version(
            document.getInteger( "main", 0 ),
            document.getInteger( "ext", 0 ) )
            : Version.UNDEFINED;
    }

    public void preStart() {
        log.debug( "starting mongo client {}, version {}", this, databaseVersion() );
        for( var migration : Migration.of( databaseName, databaseVersion(), migrations ) ) {
            log.debug( "executing migration {} for {}", migration, databaseVersion() );
            migration.execute( shell, host, port, physicalDatabase );
            updateVersion( migration.version );
        }
        log.debug( "migration complete, database is {}", databaseVersion() );
    }

    public CodecRegistry getCodecRegistry() {
        return database.getCodecRegistry();
    }

    public <T> MongoCollection<T> getCollection( String collection, Class<T> clazz ) {
        return database.getCollection( collection, clazz );
    }

    public MongoCollection<Document> getCollection( String collection ) {
        return database.getCollection( collection );
    }

    @Override
    public void close() {
        mongoClient.close();
    }

    public void updateVersion( Version version ) {
        this.getCollection( "version" ).replaceOne( new Document( "_id", "version" ),
            new Document( Map.of( "main", version.main, "ext", version.ext ) ),
            new ReplaceOptions().upsert( true ) );
    }

    public void dropDatabase() {
        log.debug( "dropping database {}", this );
        this.database.drop();
    }
}
