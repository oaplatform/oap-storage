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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by igor.petrenko on 2020-11-02.
 */
@Slf4j
public class MongoIndex {
    private final MongoCollection<?> collection;
    private final HashMap<String, IndexInfo> indexes = new HashMap<>();

    public MongoIndex( MongoCollection<?> collection ) {
        this.collection = collection;

        refresh();
    }

    private void refresh() {
        var indexes = new ArrayList<Document>();
        collection.listIndexes().into( indexes );

        log.debug( "indexes doc = {}", indexes );

        this.indexes.clear();

        for( var indexDoc : indexes ) {
            var info = new IndexInfo( indexDoc );
            this.indexes.put( info.name, info );
        }

        log.debug( "indexes = {}", this.indexes );
    }

    public void update( String indexName, List<String> keys, boolean unique ) {
        log.info( "Creating index {}, keys={}, unique={}...", indexName, keys, unique );
        var info = this.indexes.get( indexName );
        if( info != null ) {
            if( info.equals( keys, unique ) ) {
                log.info( "Creating index {}, keys={}, unique={}... Already exists", indexName, keys, unique );
                return;
            } else {
                log.info( "Delete old index {}", info );
                collection.dropIndex( indexName );
            }
        }
        collection.createIndex( Indexes.ascending( keys ), new IndexOptions().name( indexName ).unique( unique ) );
        log.info( "Creating index {}, keys={}, unique={}... Done", indexName, keys, unique );

        refresh();
    }

    public IndexInfo getInfo( String indexName ) {
        return indexes.get( indexName );
    }

    @ToString
    public static class IndexInfo {
        public final String name;
        public final boolean unique;
        public final HashMap<String, Direction> keys = new HashMap<>();

        public IndexInfo( Document document ) {
            name = document.getString( "name" );
            unique = document.getBoolean( "unique", false );

            var keyDocument = document.get( "key", Document.class );
            keyDocument.forEach( ( k, v ) -> {
                keys.put( k, ( ( Number ) v ).intValue() == 1 ? Direction.ASC : Direction.DESC );
            } );
        }

        public boolean equals( List<String> keys, boolean unique ) {
            if( unique != this.unique ) return false;

            if( this.keys.size() != keys.size() ) return false;

            for( var key : keys ) {
                var d = this.keys.get( key );
                if( d == null ) return false;
                if( d == Direction.DESC ) return false;
            }

            return true;
        }

        public enum Direction {
            ASC, DESC
        }
    }
}
