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
import oap.concurrent.scheduler.Scheduled;
import oap.concurrent.scheduler.Scheduler;
import oap.storage.Storage.DataListener.IdObject;
import oap.util.Lists;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Replicator works on the MemoryStorage internals. It's intentional.
 *
 * @param <T>
 */
@Slf4j
public class Replicator<T> implements Closeable {
    private final MemoryStorage<T> slave;
    private final ReplicationMaster<T> master;
    private final Scheduled scheduled;
    protected int batchSize = 10;

    public Replicator( MemoryStorage<T> slave, ReplicationMaster<T> master, long interval, long safeModificationTime ) {
        this.slave = slave;
        this.master = master;
        this.scheduled = Scheduler.scheduleWithFixedDelay( getClass(), interval, safeModificationTime, this::replicate );
    }

    public Replicator( MemoryStorage<T> slave, ReplicationMaster<T> master, long interval ) {
        this( slave, master, interval, 1000 );
    }

    public synchronized void replicate( long last ) {
        List<Metadata<T>> newUpdates = Lists.empty();
        for( int b = 0; b < 100000; b++ ) {
            var offset = b * batchSize;
            var updates = master.updatedSince( last, batchSize, offset );
            log.trace( "replicate {} to {} last: {}, size {}, batch {}, offset {}",
                master, slave, last, updates.size(), batchSize, offset );
            if( updates.isEmpty() ) break;
            newUpdates.addAll( updates );
        }
        log.trace( "updated objects {}", newUpdates.size() );

        List<IdObject<T>> added = new ArrayList<>();
        List<IdObject<T>> updated = new ArrayList<>();

        for( Metadata<T> metadata : newUpdates ) {
            log.trace( "replicate {}", metadata );
            var id = slave.identifier.get( metadata.object );
            boolean unmodified = slave.memory.get( id ).map( m -> m.looksUnmodified( metadata ) ).orElse( false );
            if( unmodified ) {
                log.trace( "skipping unmodified {}", id );
                continue;
            }
            if( slave.memory.put( id, Metadata.from( metadata ) ) ) added.add( new IdObject<>( id, metadata.object ) );
            else updated.add( new IdObject<>( id, metadata.object ) );
        }
        slave.fireAdded( added );
        slave.fireUpdated( updated );

        var ids = master.ids();
        log.trace( "master ids {}", ids );
        List<IdObject<T>> deleted = slave.memory.selectLiveIds()
            .filter( id -> !ids.contains( id ) )
            .map( id -> slave.memory.removePermanently( id ).map( m -> new IdObject<>( id, m.object ) ) )
            .filter( Optional::isPresent )
            .map( Optional::get )
            .toList();
        log.trace( "deleted {}", deleted );
        slave.fireDeleted( deleted );

    }

    @Override
    public void close() {
        Scheduled.cancel( scheduled );
    }
}
