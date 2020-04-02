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
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.concurrent.Threads;
import oap.concurrent.scheduler.PeriodicScheduled;
import oap.concurrent.scheduler.Scheduled;
import oap.concurrent.scheduler.Scheduler;
import oap.io.Files;
import oap.json.Binder;
import oap.reflect.TypeRef;
import oap.storage.mongo.JsonCodec;
import oap.storage.mongo.MongoClient;
import oap.storage.mongo.OplogService;
import oap.util.Pair;
import oap.util.Stream;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.joda.time.DateTimeUtils;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;
import static oap.io.IoStreams.Encoding.GZIP;
import static oap.util.Dates.FORMAT_MILLIS;
import static oap.util.Pair.__;

@Slf4j
@ToString( of = { "table", "delay" } )
public class MongoPersistence<I, T> implements Closeable, Runnable, OplogService.OplogListener {

    private static final ReplaceOptions REPLACE_OPTIONS_UPSERT = new ReplaceOptions().upsert( true );
    public static final Path DEFAULT_CRASH_DUMP_PATH = Path.of( "/tmp/mongo-persistance-crash-dump" );

    final MongoCollection<Metadata<T>> collection;
    private final Lock lock = new ReentrantLock();
    public OplogService oplogService;
    public int batchSize = 100;
    private final String table;
    private final long delay;
    private MemoryStorage<I, T> storage;
    private PeriodicScheduled scheduled;
    private final Path crashDumpPath;

    public MongoPersistence( MongoClient mongoClient, String table, long delay, MemoryStorage<I, T> storage ) {
        this( mongoClient, table, delay, storage, DEFAULT_CRASH_DUMP_PATH );
    }

    public MongoPersistence( MongoClient mongoClient, String table, long delay, MemoryStorage<I, T> storage, Path crashDumpPath ) {
        this.table = table;
        this.delay = delay;
        this.storage = storage;

        TypeRef<Metadata<T>> ref = new TypeRef<>() {};

        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
            CodecRegistries.fromCodecs( new JsonCodec<>( ref,
                m -> this.storage.identifier.get( m.object ),
                id -> this.storage.identifier.toString( id ) ) ),
            mongoClient.database.getCodecRegistry()
        );

        this.collection = mongoClient.database
            .getCollection( table, ref.clazz() )
            .withCodecRegistry( codecRegistry );
        this.crashDumpPath = crashDumpPath.resolve( table );
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
            var deletedIds = new ArrayList<I>( batchSize );
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

    private void persist( List<I> deletedIds, List<WriteModel<Metadata<T>>> list ) {
        if( !list.isEmpty() ) {
            try {
                collection.bulkWrite( list, new BulkWriteOptions().ordered( false ) );
                deletedIds.forEach( storage.memory::removePermanently );
                list.clear();
                deletedIds.clear();
            } catch( Exception e ) {
                Path filename = crashDumpPath.resolve( FORMAT_MILLIS.print( DateTimeUtils.currentTimeMillis() ) + ".json.gz" );
                log.error( "cannot persist. Dumping to " + filename + "...", e );
                List<Pair<String, Metadata<T>>> dump = Stream.of( list )
                    .filter( model -> model instanceof ReplaceOneModel )
                    .map( model -> __( "replace", ( ( ReplaceOneModel<Metadata<T>> ) model ).getReplacement() ) )
                    .toList();
                Files.writeString( filename, GZIP, Binder.json.marshal( dump ) );
            }
        }
    }

    @Override
    public void close() {
        log.debug( "closing {}...", this );
        if( scheduled != null && storage != null ) {
            Threads.synchronously( lock, () -> {
                Scheduled.cancel( scheduled );
                fsync( scheduled.lastExecuted() );

                log.debug( "closed {}...", this );
            } );
        } else log.debug( "this {} was't started or already closed", this );
    }

    @Override
    public void updated( String table, String mongoId ) {
        refresh( mongoId );
    }

    @Override
    public void deleted( String table, String mongoId ) {
        storage.delete( storage.identifier.fromString( mongoId ) );
    }

    @Override
    public void inserted( String table, String mongoId ) {
        refresh( mongoId );
    }

    private void refresh( String mongoId ) {
        var m = collection.find( eq( "_id", mongoId ) ).first();
        if( m != null ) {
            storage.lock.synchronizedOn( mongoId, () -> {
                var id = storage.identifier.fromString( mongoId );
                var old = storage.memory.get( id );
                if( old.isEmpty() || m.modified > old.get().modified ) {
                    log.debug( "refresh from mongo {}", mongoId );
                    storage.memory.put( id, m );
                    if( old.isEmpty() ) storage.fireAdded( id, m.object );
                    else storage.fireUpdated( id, m.object );
                } else log.debug( "[{}] m.modified <= oldM.modified", mongoId );
            } );
        }
    }
}
