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

import lombok.extern.slf4j.Slf4j;
import oap.application.remote.RemoteInvocationException;
import oap.concurrent.scheduler.Scheduled;
import oap.concurrent.scheduler.Scheduler;
import oap.storage.Storage.DataListener.IdObject;

import java.io.Closeable;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static oap.storage.Storage.DataListener.IdObject.__io;

/**
 * Replicator works on the MemoryStorage internals. It's intentional.
 *
 * @param <T>
 */
@Slf4j
public class Replicator<I, T> implements Closeable {
    private final MemoryStorage<I, T> slave;
    private final ReplicationMaster<I, T> master;
    private Scheduled scheduled;

    public Replicator( MemoryStorage<I, T> slave, ReplicationMaster<I, T> master, long interval, long safeModificationTime ) {
        this.slave = slave;
        this.master = master;
        this.scheduled = Scheduler.scheduleWithFixedDelay( getClass(), interval, safeModificationTime, this::replicate );
    }

    public Replicator( MemoryStorage<I, T> slave, ReplicationMaster<I, T> master, long interval ) {
        this( slave, master, interval, 1000 );
    }

    public void replicateNow() {
        log.trace( "forcing replication..." );
        scheduled.triggerNow();
    }

    public synchronized void replicate( long last ) {
        List<Metadata<T>> newUpdates;

        try( var updates = master.updatedSince( last ) ) {
            log.trace( "replicate {} to {} last: {}", master, slave, last );
            newUpdates = updates.collect( toList() );
            log.trace( "updated objects {}", newUpdates.size() );
        } catch( RemoteInvocationException e ) {
            if( e.getCause() instanceof SocketException ) {
                log.error( e.getCause().getMessage() );
                return;
            }
            throw e;
        }

        var added = new ArrayList<IdObject<I, T>>();
        var updated = new ArrayList<IdObject<I, T>>();

        for( var metadata : newUpdates ) {
            log.trace( "replicate {}", metadata );
            var id = slave.identifier.get( metadata.object );
            var unmodified = slave.memory.get( id ).map( m -> m.looksUnmodified( metadata ) ).orElse( false );
            if( unmodified ) {
                log.trace( "skipping unmodified {}", id );
                continue;
            }
            if( slave.memory.put( id, Metadata.from( metadata ) ) ) added.add( __io( id, metadata.object ) );
            else updated.add( __io( id, metadata.object ) );
        }
        slave.fireAdded( added );
        slave.fireUpdated( updated );

        var ids = master.ids();
        log.trace( "master ids {}", ids );
        List<IdObject<I, T>> deleted = slave.memory.selectLiveIds()
            .filter( id -> !ids.contains( id ) )
            .map( id -> slave.memory.removePermanently( id ).map( m -> __io( id, m.object ) ) )
            .filter( Optional::isPresent )
            .map( Optional::get )
            .toList();
        log.trace( "deleted {}", deleted );
        slave.fireDeleted( deleted );

    }

    public void preStop() {
        Scheduled.cancel( scheduled );
        scheduled = null;
    }

    @Override
    public void close() {
        Scheduled.cancel( scheduled );
    }
}
