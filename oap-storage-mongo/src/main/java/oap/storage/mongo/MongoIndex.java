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
import oap.util.Dates;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by igor.petrenko on 2020-11-02.
 */
@Slf4j
public class MongoIndex {
    private final MongoCollection<?> collection;
    private final LinkedHashMap<String, IndexInfo> indexes = new LinkedHashMap<>();

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

    public void update( String indexName, List<String> keys, boolean unique, Long expireAfterSeconds ) {
        log.info( "Creating index {}, keys={}, unique={}, expireAfterSeconds={}...",
            indexName, keys, unique, expireAfterSeconds != null ? Dates.durationToString( expireAfterSeconds ) : "-" );
        var info = this.indexes.get( indexName );
        if( info != null ) {
            if( info.equals( keys, unique, expireAfterSeconds ) ) {
                log.info( "Creating index {}, keys={}, unique={}, expireAfterSeconds={}...... Already exists",
                    indexName, keys, unique,
                    expireAfterSeconds != null ? Dates.durationToString( expireAfterSeconds ) : "-" );
                return;
            } else {
                log.info( "Delete old index {}", info );
                collection.dropIndex( indexName );
            }
        }
        var indexOptions = new IndexOptions().name( indexName ).unique( unique );
        if( expireAfterSeconds != null ) indexOptions.expireAfter( expireAfterSeconds, TimeUnit.SECONDS );
        collection.createIndex( Indexes.ascending( keys ), indexOptions );
        log.info( "Creating index {}, keys={}, unique={}, expireAfterSeconds={}...... Done",
            indexName, keys, unique, expireAfterSeconds != null ? Dates.durationToString( expireAfterSeconds ) : "-" );

        refresh();
    }

    public IndexInfo getInfo( String indexName ) {
        return indexes.get( indexName );
    }

    @ToString
    public static class IndexInfo {
        public final String name;
        public final boolean unique;
        public final Long expireAfterSeconds;
        public final LinkedHashMap<String, Direction> keys = new LinkedHashMap<>();

        public IndexInfo( Document document ) {
            name = document.getString( "name" );
            unique = document.getBoolean( "unique", false );
            expireAfterSeconds = document.getLong( "expireAfterSeconds" );

            var keyDocument = document.get( "key", Document.class );
            keyDocument.forEach( ( k, v ) -> {
                keys.put( k, ( ( Number ) v ).intValue() == 1 ? Direction.ASC : Direction.DESC );
            } );
        }

        public boolean equals( List<String> keys, boolean unique, Long expireAfterSeconds ) {
            if( unique != this.unique ) return false;

            if( this.keys.size() != keys.size() ) return false;

            for( var key : keys ) {
                var d = this.keys.get( key );
                if( d != Direction.DESC ) return false;
                if( expireAfterSeconds != this.expireAfterSeconds ) return false;
            }

            return true;
        }

        public enum Direction {
            ASC, DESC
        }
    }
}