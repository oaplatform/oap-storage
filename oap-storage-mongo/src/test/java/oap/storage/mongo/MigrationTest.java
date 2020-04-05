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

import oap.util.Maps;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static oap.storage.mongo.MigrationConfig.CONFIGURATION;
import static oap.testng.Asserts.assertString;
import static oap.testng.Asserts.contentOfTestResource;
import static oap.testng.Asserts.urlOfTestResource;
import static oap.util.Pair.__;
import static org.assertj.core.api.Assertions.assertThat;

public class MigrationTest {
    @Test
    public void migrator() {
        List<MigrationConfig> configs = List.of(
            CONFIGURATION.fromUrl( urlOfTestResource( getClass(), "config1.yaml" ) ),
            CONFIGURATION.fromUrl( urlOfTestResource( getClass(), "config2.yaml" ) )
        );
        assertThat( Migration.of( "testdb", configs ) ).containsExactly(
            new Migration( "testdb", new Version( 666 ),
                List.of( "/testdb-666.migration.js", "/testdb-666-1.migration.js" ),
                Set.of( "/common.migration.js", "/common2.migration.js" ),
                Map.of(
                    "param1", true,
                    "param2", "string",
                    "param3", true,
                    "param4", "string2"
                )
            ),
            new Migration( "testdb", new Version( 666, 2 ),
                List.of( "/testdb-666-2.migration.js" ),
                Set.of( "/common.migration.js", "/common2.migration.js" ),
                Map.of(
                    "param1", false,
                    "param2", "ext 2" )
            ),
            new Migration( "testdb", new Version( 777 ),
                List.of( "/testdb-777.migration.js" ),
                Set.of(),
                Map.of()
            )
        );
    }

    @Test
    public void toScript() {
        Migration migration = new Migration( "testdb", new Version( 666 ),
            List.of( "/oap/storage/mongo/MigrationTest/s1.migration.js", "/oap/storage/mongo/MigrationTest/s2.migration.js" ),
            Set.of( "/oap/storage/mongo/MigrationTest/lib.migration.js" ),
            Maps.of(
                __( "param1", true ),
                __( "param2", "string" ),
                __( "param3", true ),
                __( "param4", "string2" )
            )
        );
        assertString( migration.toScript( "localhost", 27017 ) ).isEqualTo( contentOfTestResource( getClass(), "result.js" ) );
    }
}
