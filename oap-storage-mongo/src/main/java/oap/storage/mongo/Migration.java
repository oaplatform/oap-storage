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

import com.google.common.collect.ListMultimap;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.io.Resources;
import oap.util.BiStream;
import oap.util.Stream;
import org.apache.commons.lang.StringUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static oap.io.content.ContentReader.ofString;
import static oap.util.Maps.Collectors.toListMultimap;
import static oap.util.Pair.__;

@ToString
@EqualsAndHashCode
@Slf4j
public class Migration {
    public final Version version;
    public final List<String> scripts;
    public final Set<String> includes;
    public final Map<String, Object> params;

    public Migration( Version version, List<String> scripts, Set<String> includes, Map<String, Object> params ) {
        this.version = version;
        this.scripts = scripts;
        this.includes = includes;
        this.params = params;
    }

    public static List<Migration> of( String database, Version version, List<MigrationConfig> configs ) {
        ListMultimap<Version, MigrationConfig.Migration> migratons = Stream.of( configs )
            .flatMap( config -> Stream.of( config.migrations ) )
            .map( databases -> databases.getOrDefault( database, List.of() ) )
            .flatMap( Stream::of )
            .mapToPairs( m -> __( m.version, m ) )
            .collect( toListMultimap() );
        return BiStream.of( migratons.asMap() )
            .sorted( Comparator.comparing( p -> p._1 ) )
            .filter( p -> version.before( p._1 ) )
            .map( p -> new Migration( p._1,
                Stream.of( p._2 ).map( m -> m.script ).toList(),
                Stream.of( p._2 ).flatMap( m -> Stream.of( m.includes ) ).toSet(),
                Stream.of( p._2 ).flatMap( m -> BiStream.of( m.parameters ) ).mapToPairs( Function.identity() ).toMap()
            ) )
            .toList();
    }

    public String toScript( String mongoHost, int mongoPort, String database, String user, String password ) {
        StringBuilder script = new StringBuilder();
        script.append( "conn = new Mongo(\"" ).append( mongoHost ).append( ":" ).append( mongoPort ).append( "\");\n" );
        script.append( "db = conn.getDB(\"" ).append( database ).append( "\");\n" );
        if( StringUtils.isNotEmpty( user ) && StringUtils.isNotEmpty( password ) ) {
            script.append( "db.auth(\"" ).append( user ).append( "\",\"" ).append( password ).append( "\");\n" );
        }
        script.append( "\n// ========== PARAMETERS ==========\n" );
        params.forEach( ( n, v ) -> script.append( "var " ).append( n ).append( " = " )
            .append( v instanceof String ? "\"" + v + "\"" : v ).append( ";\n" ) );
        for( String include : includes ) {
            script.append( "\n// ========== INCLUDE: " ).append( include ).append( " ==========\n" );
            script.append( Resources.readOrThrow( getClass(), include, ofString() ) );
        }
        for( String scr : scripts ) {
            script.append( "\n// ========== SCRIPT: " ).append( scr ).append( " ==========\n" );
            script.append( "(function() {\n" );
            script.append( Resources.readOrThrow( getClass(), scr, ofString() ) );
            script.append( "})();\n" );
        }
        return script.toString();
    }

    public String toScript( String mongoHost, int mongoPort, String database ) {
        return toScript( mongoHost, mongoPort, database, null, null );
    }

    public void execute( MongoShell shell, String mongoHost, int mongoPort, String database ) {
        execute( shell, mongoHost, mongoPort, database, null, null );
    }

    public void execute( MongoShell shell, String mongoHost, int mongoPort, String database, String user, String password ) {
        String script = toScript( mongoHost, mongoPort, database, user, password );
        log.debug( "executing migration of {} to {} with script\n{}", database, version, script );
        shell.execute( mongoHost, mongoPort, database, script );
    }
}
