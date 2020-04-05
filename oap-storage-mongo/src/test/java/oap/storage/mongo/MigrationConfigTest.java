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

import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class MigrationConfigTest {
    @Test
    public void parse() {
        MigrationConfig config = MigrationConfig.CONFIGURATION.fromResource( getClass(), getClass().getSimpleName() + "/config.yaml" );
        assertThat( config ).isEqualTo( new MigrationConfig(
            Map.of( "testdb", List.of(
                new MigrationConfig.Migration(
                    new Version( 666 ),
                    "/testdb-666.migration.js",
                    List.of( "/common.migration.js", "/common2.migration.js" ),
                    Map.of( "param1", true, "param2", "string" )
                ),
                new MigrationConfig.Migration(
                    new Version( 666 ),
                    "/testdb-666-1.migration.js",
                    List.of( "/common.migration.js", "/common2.migration.js" ),
                    Map.of( "param1", true, "param2", "string" )
                ),
                new MigrationConfig.Migration(
                    new Version( 666, 2 ),
                    "/testdb-666-2.migration.js",
                    List.of( "/common.migration.js", "/common2.migration.js" ),
                    Map.of( "param1", false, "param2", "ext 2" )
                ),
                new MigrationConfig.Migration(
                    new Version( 777 ),
                    "/testdb-777.migration.js"
                )
            ) )
        ) );
    }
}
