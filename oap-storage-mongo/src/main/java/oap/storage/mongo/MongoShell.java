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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import oap.io.Files;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static java.util.Arrays.asList;

@Slf4j
public class MongoShell {
    public static final String[] SHELL_LOCATIONS = {
        "/usr/bin/mongo",
        "/usr/bin/mongosh",
        "/usr/local/bin/mongo",
        "/usr/local/bin/mongosh",
        "/usr/local/opt/mongodb-community/bin/mongo",
        "/usr/local/opt/mongodb-community/bin/mongosh"
    };
    private final String path;

    public MongoShell() {
        this( Files.resolve( SHELL_LOCATIONS )
            .map( Path::toString )
            .orElseGet( () -> {
                log.warn( "can't find mongo shell at " + asList( SHELL_LOCATIONS ) );
                log.warn( "defaulting to mongo executable" );
                return "mongosh";
            } ) );
    }

    public MongoShell( String path ) {
        this.path = path;
    }

    @SneakyThrows
    public void execute( String host, int port, String database, String script ) {
        var file = File.createTempFile( "migration_" + database + "_", ".js" );
        file.deleteOnExit();
        Files.writeString( file.toPath(), script );
        log.debug( "script file {}", file );
        execute( host, port, database, file.toPath() );
    }

    @SneakyThrows
    public void execute( String host, int port, String database, Path scriptFile ) {
        var cmd = new CommandLine( path );
        cmd.addArgument( "--verbose" );
        cmd.addArgument( host + ":" + port + "/" + database );
        cmd.addArgument( scriptFile.toString() );
        log.debug( "executing {}", cmd );
        var executor = new DefaultExecutor();
        var los = new LogOutputStream( 1 ) {
            @Override
            protected void processLine( String line, int level ) {
                log.debug( line );
            }
        };
        executor.setStreamHandler( new PumpStreamHandler( los, los ) );
        executor.setExitValues( null );
        var exitCode = executor.execute( cmd );

        if( exitCode != 0 ) throw new IOException( cmd + " exited with code " + exitCode );
    }
}
