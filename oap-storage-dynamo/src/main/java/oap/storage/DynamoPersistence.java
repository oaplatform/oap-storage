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

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.application.ServiceName;
import oap.concurrent.Threads;
import oap.concurrent.scheduler.ScheduledExecutorService;
import oap.dynamodb.DynamodbClient;
import oap.dynamodb.Key;
import oap.dynamodb.crud.AbstractOperation;
import oap.dynamodb.crud.BatchOperationHelper;
import oap.dynamodb.crud.DeleteItemOperation;
import oap.dynamodb.crud.OperationType;
import oap.dynamodb.crud.UpdateItemOperation;
import oap.dynamodb.streams.DynamodbStreamsRecordProcessor;
import oap.io.Closeables;
import oap.io.Files;
import oap.json.Binder;
import oap.util.Dates;
import oap.util.Pair;
import oap.util.Stream;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static oap.concurrent.Threads.synchronizedOn;
import static oap.io.IoStreams.Encoding.GZIP;
import static oap.util.Pair.__;

@Slf4j
@ToString( of = { "tableName", "delay" } )
public class DynamoPersistence<I, T> implements Closeable {

    public static final Path DEFAULT_CRASH_DUMP_PATH = Path.of( "/tmp/dynamo-persistance-crash-dump" );
    public static final DateTimeFormatter CRASH_DUMP_PATH_FORMAT_MILLIS = DateTimeFormat
        .forPattern( "yyyy-MM-dd-HH-mm-ss-SSS" )
        .withZoneUTC();
    private final Lock lock = new ReentrantLock();
    private final DynamodbClient dynamodbClient;
    private final DynamodbStreamsRecordProcessor streamProcessor;
    private final BatchOperationHelper batchWriter;
    private final String tableName;
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
    private final Function<Map<String, AttributeValue>, Metadata<T>> convertFromDynamoItem;
    private final Function<Metadata<T>, Map<String, Object>> convertToDynamoItem;

    public DynamoPersistence( DynamodbClient dynamodbClient,
                              String tableName,
                              long delay,
                              MemoryStorage<I, T> storage,
                              Function<Map<String, AttributeValue>, Metadata<T>> convertFromDynamoItem,
                              Function<Metadata<T>, Map<String, Object>> convertToDynamoItem ) {
        this( dynamodbClient, tableName, delay, storage, convertFromDynamoItem, convertToDynamoItem, DEFAULT_CRASH_DUMP_PATH );
    }

    public DynamoPersistence( DynamodbClient dynamodbClient,
                              String tableName, long delay,
                              MemoryStorage<I, T> storage,
                              Function<Map<String, AttributeValue>, Metadata<T>> convertFromDynamoItem,
                              Function<Metadata<T>, Map<String, Object>> convertToDynamoItem,
                              Path crashDumpPath ) {
        this.convertFromDynamoItem = convertFromDynamoItem;
        this.convertToDynamoItem = convertToDynamoItem;
        this.tableName = tableName;
        this.delay = delay;
        this.storage = storage;
        this.crashDumpPath = crashDumpPath;
        this.streamProcessor = DynamodbStreamsRecordProcessor.builder( dynamodbClient.getStreamClient() ).build();
        ;
        batchWriter = new BatchOperationHelper( dynamodbClient );
        this.dynamodbClient = dynamodbClient;
    }

    public void preStart() {
        log.info( "table = {}, fsync delay = {}, watch = {}, crashDumpPath = {}",
            tableName, Dates.durationToString( delay ), watch, crashDumpPath );

        synchronizedOn( lock, () -> {
            this.load();

            scheduler = oap.concurrent.Executors.newScheduledThreadPool( 1, serviceName );
            scheduler.scheduleWithFixedDelay( this::fsync, delay, delay, TimeUnit.MILLISECONDS );
        } );

        if( watch ) {
            dynamodbClient.update( new Key( tableName, "id", "dummy" ), "dummy_name", "John Doe" );
            dynamodbClient.delete( new Key( tableName, "id", "dummy" ), null );
            watchExecutor = Executors.newSingleThreadExecutor();

            watchExecutor.execute( () -> {
                Threads.notifyAllFor( watchExecutor );
                TableDescription table = dynamodbClient.describeTable( tableName, null );
                String streamArn = table.latestStreamArn();
                streamProcessor.processRecords( streamArn, record -> {
                    log.trace( "dynamoDb notification: {} ", record );
                    var key = record.dynamodb().keys().get( "id" );
                    var op = record.eventName();

                    if( key == null ) return;
                    var id = key.s();
                    if( id == null ) return;

                    switch( op ) {
                        case REMOVE -> deleteById( id );
                        case INSERT, MODIFY -> refreshById( id );
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
        log.debug( "loading data from {}", tableName );
        Consumer<Metadata<T>> cons = metadata -> storage.memory.put( storage.identifier.get( metadata.object ), metadata );
        log.info( "Load items from [{}] DynamoDB table", tableName );
        dynamodbClient.getItemsByScan( tableName, null ).map( convertFromDynamoItem ).forEach( cons );
        log.info( storage.size() + " object(s) loaded." );
    }

    private void fsync() {
        var time = DateTimeUtils.currentTimeMillis();
        synchronizedOn( lock, () -> {
            log.trace( "fsyncing, last: {}, objects in storage: {}", lastExecuted, storage.size() );
            var list = new ArrayList<AbstractOperation>( batchSize );
            var deletedIds = new ArrayList<I>( batchSize );
            storage.memory.selectUpdatedSince( lastExecuted ).forEach( ( id, m ) -> {
                if( m.isDeleted() ) {
                    deletedIds.add( id );
                    list.add( new DeleteItemOperation( new Key( tableName, "id", id.toString() ) ) );
                } else
                    list.add( new UpdateItemOperation( new Key( tableName, "id", id.toString() ), convertToDynamoItem.apply( m ) ) );
                if( list.size() >= batchSize ) persist( deletedIds, list );
            } );
            persist( deletedIds, list );
            lastExecuted = time;
        } );
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

    private void persist( List<I> deletedIds, List<AbstractOperation> list ) {
        if( !list.isEmpty() ) try {
            batchWriter.addOperations( list );
            batchWriter.write();
            deletedIds.forEach( storage.memory::removePermanently );
            list.clear();
            deletedIds.clear();
        } catch( Exception e ) {
            Path filename = crashDumpPath.resolve( CRASH_DUMP_PATH_FORMAT_MILLIS.print( DateTimeUtils.currentTimeMillis() ) + ".json.gz" );
            log.error( "cannot persist. Dumping to " + filename + "...", e );
            List<Pair<String, AbstractOperation>> dump = Stream.of( list )
                .filter( model -> model.getType().equals( OperationType.UPDATE ) )
                .map( model -> __( "replace", model ) )
                .toList();
            Files.writeString( filename, GZIP, Binder.json.marshal( dump ) );
        }
    }

    private void refreshById( String dynamoId ) {
        var res = dynamodbClient.get( new Key( tableName, "id", dynamoId ) );
        if( res != null && res.isSuccess() ) {
            Metadata<T> m = convertFromDynamoItem.apply( res.getSuccessValue() );
            storage.lock.synchronizedOn( dynamoId, () -> {
                var id = storage.identifier.fromString( dynamoId );
                var old = storage.memory.get( id );
                if( old.isEmpty() || m.modified > old.get().modified ) {
                    log.debug( "refresh from dynamo {}", dynamoId );
                    storage.memory.put( id, m );
                    if( old.isEmpty() ) storage.fireAdded( id, m.object );
                    else storage.fireUpdated( id, m.object );
                } else log.debug( "[{}] m.modified <= oldM.modified", dynamoId );
            } );
        }
    }
}
