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
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.application.ServiceName;
import oap.concurrent.Threads;
import oap.concurrent.scheduler.ScheduledExecutorService;
import oap.io.Closeables;
import oap.io.Files;
import oap.json.Binder;
import oap.reflect.TypeRef;
import oap.storage.mongo.JsonCodec;
import oap.storage.mongo.MongoClient;
import oap.util.Dates;
import oap.util.Pair;
import oap.util.Stream;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;
import static oap.concurrent.Threads.synchronizedOn;
import static oap.io.IoStreams.Encoding.GZIP;
import static oap.util.Pair.__;

@Slf4j
@ToString( of = { "collectionName", "delay" } )
public class MongoPersistence<I, T> implements Closeable {

    public static final Path DEFAULT_CRASH_DUMP_PATH = Path.of( "/tmp/mongo-persistance-crash-dump" );
    public static final DateTimeFormatter CRASH_DUMP_PATH_FORMAT_MILLIS = DateTimeFormat
        .forPattern( "yyyy-MM-dd-HH-mm-ss-SSS" )
        .withZoneUTC();
    private static final ReplaceOptions REPLACE_OPTIONS_UPSERT = new ReplaceOptions().upsert( true );
    final MongoCollection<Metadata<T>> collection;
    private final Lock lock = new ReentrantLock();
    private final MongoClient mongoClient;
    private final String collectionName;
    private final long delay;
    private final MemoryStorage<I, T> storage;
    private final Path crashDumpPath;
    @ServiceName
    public String serviceName;
    public boolean watch = false;
    protected int batchSize = 100;
    private ExecutorService watchExecutor;
    private ScheduledExecutorService scheduler;
    private volatile long lastExecuted = -1;

    public MongoPersistence( MongoClient mongoClient, String collectionName, long delay, MemoryStorage<I, T> storage ) {
        this( mongoClient, collectionName, delay, storage, DEFAULT_CRASH_DUMP_PATH );
    }

    public MongoPersistence( MongoClient mongoClient, String collectionName, long delay, MemoryStorage<I, T> storage, Path crashDumpPath ) {
        this.mongoClient = mongoClient;
        this.collectionName = collectionName;
        this.delay = delay;
        this.storage = storage;

        TypeRef<Metadata<T>> ref = new TypeRef<>() {};

        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
            CodecRegistries.fromCodecs( new JsonCodec<>( ref,
                m -> this.storage.identifier.get( m.object ),
                this.storage.identifier::toString ) ),
            mongoClient.getCodecRegistry()
        );

        this.collection = mongoClient
            .getCollection( collectionName, ref.clazz() )
            .withCodecRegistry( codecRegistry );
        this.crashDumpPath = crashDumpPath.resolve( collectionName );
    }

    public void preStart() {
        log.info( "collection = {}, fsync delay = {}, watch = {}, crashDumpPath = {}",
            collectionName, Dates.durationToString( delay ), watch, crashDumpPath );

        synchronizedOn( lock, () -> {
            this.load();

            scheduler = oap.concurrent.Executors.newScheduledThreadPool( 1, serviceName );
            scheduler.scheduleWithFixedDelay( this::fsync, delay, delay, TimeUnit.MILLISECONDS );
        } );

        if( watch ) {
            watchExecutor = Executors.newSingleThreadExecutor();

            watchExecutor.execute( () -> {
                var changeStreamDocuments = mongoClient.getCollection( collectionName ).withReadConcern( ReadConcern.MAJORITY ).watch();

                Threads.notifyAllFor( watchExecutor );

                changeStreamDocuments.forEach( ( Consumer<? super ChangeStreamDocument<Document>> ) csd -> {
                    log.trace( "mongo notification: {} ", csd );
                    var op = csd.getOperationType();
                    var key = csd.getDocumentKey();
                    if( key == null ) return;

                    var bid = key.getString( "_id" );
                    if( bid == null ) return;

                    var id = bid.getValue();
                    switch( op ) {
                        case DELETE -> deleteById( id );
                        case INSERT, UPDATE -> refreshById( id );
                    }
                } );
            } );

            Threads.waitFor( watchExecutor );
        }
    }

    private Optional<T> deleteById( String id ) {
        return storage.delete( storage.identifier.fromString( id ) );
    }

    private void load() {
        MongoNamespace namespace = collection.getNamespace();
        log.debug( "loading data from {}", namespace );
        Consumer<Metadata<T>> cons = metadata -> storage.memory.put( storage.identifier.get( metadata.object ), metadata );
        log.info( "Load {} documents from [{}] Mongo namespace", collection.countDocuments(), namespace );
        collection.find().forEach( cons );
        log.info( storage.size() + " object(s) loaded." );
    }

    private void fsync() {
        var time = DateTimeUtils.currentTimeMillis();
        synchronizedOn( lock, () -> {
            log.trace( "fsyncing, last: {}, objects in storage: {}", lastExecuted, storage.size() );
            var list = new ArrayList<WriteModel<Metadata<T>>>( batchSize );
            var deletedIds = new ArrayList<I>( batchSize );
            storage.memory.selectUpdatedSince( lastExecuted ).forEach( ( id, m ) -> {
                if( m.isDeleted() ) {
                    deletedIds.add( id );
                    list.add( new DeleteOneModel<>( eq( "_id", id ) ) );
                } else list.add( new ReplaceOneModel<>( eq( "_id", id ), m, REPLACE_OPTIONS_UPSERT ) );
                if( list.size() >= batchSize ) persist( deletedIds, list );
            } );
            persist( deletedIds, list );
            lastExecuted = time;
        } );
    }

    private void persist( List<I> deletedIds, List<WriteModel<Metadata<T>>> list ) {
        if( !list.isEmpty() ) try {
            collection.bulkWrite( list, new BulkWriteOptions().ordered( false ) );
            deletedIds.forEach( storage.memory::removePermanently );
            list.clear();
            deletedIds.clear();
        } catch( Exception e ) {
            Path filename = crashDumpPath.resolve( CRASH_DUMP_PATH_FORMAT_MILLIS.print( DateTimeUtils.currentTimeMillis() ) + ".json.gz" );
            log.error( "cannot persist. Dumping to " + filename + "...", e );
            List<Pair<String, Metadata<T>>> dump = Stream.of( list )
                .filter( model -> model instanceof ReplaceOneModel )
                .map( model -> __( "replace", ( ( ReplaceOneModel<Metadata<T>> ) model ).getReplacement() ) )
                .toList();
            Files.writeString( filename, GZIP, Binder.json.marshal( dump ) );
        }
    }

    @Override
    public void close() {
        log.debug( "closing {}...", this );
        if( scheduler != null && storage != null ) synchronizedOn( lock, () -> {
            Closeables.close( scheduler );
            fsync();
            log.debug( "closed {}...", this );
        } );
        else log.debug( "this {} was't started or already closed", this );

        if( watch ) Closeables.close( watchExecutor );
    }

    private void refreshById( String mongoId ) {
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
