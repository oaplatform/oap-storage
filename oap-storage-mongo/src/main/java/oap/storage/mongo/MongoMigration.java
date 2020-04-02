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

import com.google.common.base.Preconditions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import lombok.extern.slf4j.Slf4j;
import oap.io.Files;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

@Slf4j
public class MongoMigration implements Migration {
    private static final FindOneAndUpdateOptions UPSERT = new FindOneAndUpdateOptions().upsert( true );

    private final Path directory;
    private final String mongoShell;
    public HashMap<String, String> variables = new HashMap<>();

    public MongoMigration( Path directory ) {
        this.directory = directory;
        mongoShell = MongoFixture.MONGO_SHELL;
        Preconditions.checkNotNull( mongoShell, "MONGO_SHELL is not defined" );
    }

    private void nextMigration( MongoClient client, int fromVersion, String functions ) throws IOException {
        log.info( "directory {} ...", directory );
        var versionDirectory = directory.resolve( String.valueOf( fromVersion ) );
        log.debug( "try version directory {} ...", versionDirectory );
        if( java.nio.file.Files.isDirectory( versionDirectory ) ) {
            log.info( "{} exists", versionDirectory );
            var files = Files.fastWildcard( versionDirectory, "*.js" );
            var sb = new StringBuilder();

            var vars = variables
                .entrySet()
                .stream()
                .map( entry -> "var " + entry.getKey() + " = " + entry.getValue() + ";" )
                .collect( Collectors.joining( "\n" ) );

            sb.append( "conn = new Mongo(\"" ).append( client.host ).append( ":" ).append( client.port ).append( "\");\n" )
                .append( "db = conn.getDB(\"" ).append( client.database.getName() ).append( "\");\n" )
//                .append( "session = db.getMongo().startSession( { readPreference: { mode: \"primary\" } } );\n" )
//                .append( "session.startTransaction( { readConcern: { level: \"local\" }, writeConcern: { w: \"majority\" } } );\n" )
//                .append( "try {\n" )
                .append( vars ).append( "\n" )
                .append( functions ).append( "\n" );

            for( var i = 0; i < files.size(); i++ ) {
                var file = files.get( i );

                log.info( "file {} ...", file );

                sb.append( "function func" ).append( i ).append( "() {\n" )
                    .append( Files.readString( file ) )
                    .append( "\n}\n" )
                    .append( "func" ).append( i ).append( "();\n" );
            }

//            sb.append( """
//                } catch(error) {
//                  session.abortTransaction();
//                  throw error;
//                }
//
//                session.commitTransaction();
//                session.endSession();
//
//                """.stripIndent() );

            var tmpFile = File.createTempFile( "mongo", "migration" );
            tmpFile.deleteOnExit();

            log.trace( "evar = {}", sb.toString() );
            Files.writeString( tmpFile.getPath(), sb.toString() );

            var commandline = new CommandLine( mongoShell );
            commandline.addArgument( "--verbose" );
            commandline.addArgument( client.host + ":" + client.port + "/" + client.database.getName() );
            commandline.addArgument( tmpFile.toString() );
            log.debug( "migration cmd: {}", commandline );
            var defaultExecutor = new DefaultExecutor();
            var los = new LogOutputStream( 1 ) {
                @Override
                protected void processLine( String line, int level ) {
                    log.debug( line );
                }
            };
            defaultExecutor.setStreamHandler( new PumpStreamHandler( los, los ) );
            defaultExecutor.setExitValues( null );
            var response = defaultExecutor.execute( commandline );

            if( response != 0 )
                throw new MigrationExceptin( "code: " + response );

            log.info( "directory {} ... Done", versionDirectory );
        }
    }

    @Override
    public void run( MongoClient client ) throws IOException {
        var toVersion = Integer.parseInt( Files.readString( directory.resolve( "version.txt" ) ).trim() );
        var versionCollection = client.database.getCollection( "version" );
        Document versionDocument = versionCollection.find( eq( "_id", "version" ) ).first();
        if( versionDocument == null ) {
            versionDocument = new Document( Map.of( "_id", "version", "value", 0 ) );
        }
        int fromVersion = versionDocument.getInteger( "value" );

        log.info( "migration version = {}, database version = {}", toVersion, fromVersion );

        String func = "";
        final Path functions = directory.resolve( "functions.js" );
        if( java.nio.file.Files.exists( functions ) ) func = Files.readString( functions );

        while( toVersion >= fromVersion + 1 ) {
            log.info( "migration from {} to {}", fromVersion, fromVersion + 1 );
            fromVersion = fromVersion + 1;
            nextMigration( client, fromVersion, func );
            versionCollection.findOneAndUpdate( eq( "_id", "version" ), set( "value", fromVersion ), UPSERT );
        }
    }
}
