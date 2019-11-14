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

package oap.storage;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.Threads;
import oap.concurrent.scheduler.PeriodicScheduled;
import oap.concurrent.scheduler.Scheduled;
import oap.concurrent.scheduler.Scheduler;
import oap.json.Binder;
import oap.reflect.TypeRef;
import oap.storage.mongo.JsonCodec;
import oap.storage.mongo.MongoClient;
import oap.storage.mongo.OplogService;
import oap.util.Try;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.Closeable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;

@Slf4j
public class MongoPersistence<T> implements Closeable, Runnable, OplogService.OplogListener {

    public static final ReplaceOptions REPLACE_OPTIONS_UPSERT = new ReplaceOptions().upsert( true );
    public static final String errFile = new SimpleDateFormat( "yyyy-MM-dd-HH-mm-ss'_failed.json'" ).format( new Date() );

    public final MongoCollection<Metadata<T>> collection;
    private final Lock lock = new ReentrantLock();
    public OplogService oplogService;
    public int batchSize = 100;
    private long delay;
    private MemoryStorage<T> storage;
    private PeriodicScheduled scheduled;
    private Path errObjectPath = Path.of( "/tmp", errFile );
    private long errFileExpiration = 86400000;

    @Deprecated
    public MongoPersistence( MongoClient mongoClient,
                             String table,
                             long delay,
                             MemoryStorage<T> storage ) {
        this.delay = delay;
        this.storage = storage;

        TypeRef<Metadata<T>> ref = new TypeRef<>() {};

        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
            CodecRegistries.fromCodecs( new JsonCodec<>( ref, m -> this.storage.identifier.get( m.object ) ) ),
            mongoClient.database.getCodecRegistry()
        );

        this.collection = mongoClient.database
            .getCollection( table, ref.clazz() )
            .withCodecRegistry( codecRegistry );
    }

    public MongoPersistence( MongoClient mongoClient,
                             String table,
                             long delay,
                             MemoryStorage<T> storage,
                             String dirForFailures,
                             int errFileExpiration ) {
        this( mongoClient, table, delay, storage );
        this.errObjectPath = Path.of( dirForFailures, errFile );
        this.errFileExpiration = errFileExpiration;
    }

    @Override
    public void run() {
        fsync( scheduled.lastExecuted() );
    }

    public void start() {
        Threads.synchronously( lock, () -> {
            this.load();
            this.scheduled = Scheduler.scheduleWithFixedDelay( getClass(), delay, this::fsync );
            if( oplogService != null ) oplogService.addListener( collection.getNamespace().getCollectionName(), this );
        } );
    }

    private void load() {
        MongoNamespace namespace = collection.getNamespace();
        log.debug( "loading data from {}", namespace );
        Consumer<Metadata<T>> cons = metadata -> storage.memory.put( storage.identifier.get( metadata.object ), metadata );
        log.info( "Load {} documents from [{}] Mongo namespace", collection.countDocuments(), namespace );
        collection.find().forEach( cons );
        log.info( storage.size() + " object(s) loaded." );
    }

    private void fsync( long last ) {
        Threads.synchronously( lock, () -> {
            log.trace( "fsyncing, last: {}, objects in storage: {}", last, storage.size() );
            var list = new ArrayList<WriteModel<Metadata<T>>>( batchSize );
            var deletedIds = new ArrayList<String>( batchSize );
            storage.memory.selectUpdatedSince( last ).forEach( ( id, m ) -> {
                if( m.isDeleted() ) {
                    deletedIds.add( id );
                    list.add( new DeleteOneModel<>( eq( "_id", id ) ) );
                } else list.add( new ReplaceOneModel<>( eq( "_id", id ), m, REPLACE_OPTIONS_UPSERT ) );
                if( list.size() >= batchSize ) persist( deletedIds, list );
            } );
            persist( deletedIds, list );
        } );
    }

    private void persist( List<String> deletedIds, List<WriteModel<Metadata<T>>> list ) {
        if( !list.isEmpty() ) {
            try {
                collection.bulkWrite( list, new BulkWriteOptions().ordered( false ) );
            } catch( BsonMaximumSizeExceededException e ) {
                // store file to local FS, which wasn't persisted to MongoDB
                for( WriteModel<Metadata<T>> model : list ) {
                    if( model instanceof ReplaceOneModel ) {
                        Binder.json.marshal( errObjectPath, ( ( ReplaceOneModel<Metadata<T>> ) model ).getReplacement().object );
                    }
                }
                throw e;
            }
            deletedIds.forEach( storage.memory::removePermanently );
            deletedIds.clear();
            list.clear();
        }
    }

    /**
     * Perform fsync.</p>
     * Remove old error JSON files, which were not persisted to MongoDB
     */
    @Override
    public void close() {
        log.debug( "Closing {}...", this );
        if( scheduled != null && storage != null ) {
            Threads.synchronously( lock, () -> {
                Scheduled.cancel( scheduled );
                fsync( scheduled.lastExecuted() );
                try {
                    Files.walk( errObjectPath.getParent() )
                        .map( Path::toString )
                        .filter( f -> f.endsWith( "_failed.json" ) )
                        .map( s -> Path.of( s ) )
                        .filter( Try.filter( path -> Files.getLastModifiedTime( path ).toMillis() > errFileExpiration ) )
                        .forEach( Try.consume( Files::delete ) );
                } catch( UncheckedIOException e ) {
                    log.warn( "Failure while removing old failed files.", e );
                }
                log.debug( "Closed {}", this );
            } );
        } else {
            log.debug( "This {} was't started or already closed", this );
        }
    }

    @Override
    public void updated( String table, String id ) {
        refresh( id );
    }

    @Override
    public void deleted( String table, String id ) {
        storage.delete( id );
    }

    @Override
    public void inserted( String table, String id ) {
        refresh( id );
    }

    public void refresh( String id ) {
        var m = collection.find( eq( "_id", id ) ).first();
        if( m != null ) {
            storage.lock.synchronizedOn( id, () -> {
                var old = storage.memory.get( id );
                if( old.isEmpty() || m.modified > old.get().modified ) {
                    log.debug( "refresh from mongo {}", id );
                    storage.memory.put( id, m );
                    if( old.isEmpty() ) storage.fireAdded( id, m.object );
                    else storage.fireUpdated( id, m.object );
                } else log.debug( "[{}] m.modified <= oldM.modified", id );
            } );
        }
    }
}
