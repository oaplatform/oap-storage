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

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.application.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode
@ToString
public class MigrationConfig {
    public static final Configuration<MigrationConfig> CONFIGURATION = new Configuration<>( MigrationConfig.class, "oap-mongo-migration" );
    public final Map<String, List<Migration>> migrations;

    @JsonCreator
    public MigrationConfig( Map<String, List<Migration>> migrations ) {
        this.migrations = migrations;
    }

    @EqualsAndHashCode
    @ToString
    public static class Migration {
        public final Version version;
        public final String script;
        public final List<String> includes = new ArrayList<>();
        public final Map<String, Object> parameters = new HashMap<>();

        @JsonCreator
        public Migration( Version version, String script ) {
            this( version, script, List.of(), Map.of() );
        }

        public Migration( Version version, String script, List<String> includes, Map<String, Object> parameters ) {
            this.version = version;
            this.script = script;
            this.includes.addAll( includes );
            this.parameters.putAll( parameters );
        }
    }
}
