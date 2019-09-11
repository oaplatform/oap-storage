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
import oap.storage.Storage.DataListener.IdObject;
import oap.util.BiStream;
import oap.util.Lists;
import oap.util.Pair;
import oap.util.Stream;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static oap.storage.Storage.DataListener.IdObject.__io;

@Slf4j
public class MemoryStorage<T> implements Storage<T>, ReplicationMaster<T> {
    public final Identifier<T> identifier;
    protected final Lock lock;
    protected final List<DataListener<T>> dataListeners = new CopyOnWriteArrayList<>();
    protected final Memory<T> memory;

    public MemoryStorage( Identifier<T> identifier, Lock lock ) {
        this.identifier = identifier;
        this.lock = lock;
        this.memory = new Memory<>( lock );
    }

    @Override
    public Stream<T> select() {
        return memory.selectLive().map( p -> p._2.object );
    }

    @Override
    public List<T> list() {
        return select().toList();
    }

    @Override
    public T store( @Nonnull T object ) {
        String id = identifier.getOrInit( object, this::get );
        lock.synchronizedOn( id, () -> {
            if( memory.put( id, object ) ) fireAdded( id, object );
            else fireUpdated( id, object );
        } );
        return object;
    }

    @Override
    public void store( Collection<T> objects ) {
        List<IdObject<T>> added = new ArrayList<>();
        List<IdObject<T>> updated = new ArrayList<>();

        for( T object : objects ) {
            String id = identifier.getOrInit( object, this::get );
            lock.synchronizedOn( id, () -> {
                if( memory.put( id, object ) ) added.add( __io( id, object ) );
                else updated.add( __io( id, object ) );
            } );
        }
        fireAdded( added );
        fireUpdated( updated );
    }

    @Override
    public Optional<T> update( @Nonnull String id, @Nonnull Function<T, T> update ) {
        requireNonNull( id );
        Optional<Metadata<T>> result = memory.remap( id, update );
        result.ifPresent( m -> fireUpdated( id, m.object ) );
        return result.map( m -> m.object );
    }

    @Override
    public T update( String id, @Nonnull Function<T, T> update, @Nonnull Supplier<T> init ) {
        if( id == null ) return store( init.get() );
        else return lock.synchronizedOn( id, () -> update( id, update ).orElseGet( () -> store( init.get() ) ) );
    }

    @Override
    public Optional<T> get( @Nonnull String id ) {
        return memory.get( id ).map( m -> m.object );
    }

    @Override
    public void deleteAll() {
        fireDeleted( Lists.map( memory.markDeletedAll(), p -> __io( p._1, p._2.object ) ) );
    }

    public Optional<T> delete( @Nonnull String id ) {
        requireNonNull( id );
        Optional<T> old = memory.markDeleted( id ).map( m -> m.object );
        old.ifPresent( o -> fireDeleted( id, o ) );
        return old;
    }


    @Override
    public long size() {
        return memory.selectLiveIds().count();
    }

    protected void fireAdded( String id, T object ) {
        for( DataListener<T> dataListener : this.dataListeners )
            dataListener.added( List.of( __io( id, object ) ) );
    }

    protected void fireUpdated( String id, T object ) {
        for( DataListener<T> dataListener : this.dataListeners )
            dataListener.updated( List.of( __io( id, object ) ) );
    }

    protected void fireAdded( List<IdObject<T>> objects ) {
        if( !objects.isEmpty() )
            for( DataListener<T> dataListener : this.dataListeners ) dataListener.added( objects );
    }

    protected void fireUpdated( List<IdObject<T>> objects ) {
        if( !objects.isEmpty() )
            for( DataListener<T> dataListener : this.dataListeners ) dataListener.updated( objects );
    }

    protected void fireDeleted( List<IdObject<T>> objects ) {
        if( !objects.isEmpty() )
            for( DataListener<T> dataListener : this.dataListeners ) dataListener.deleted( objects );
    }

    protected void fireDeleted( String id, T object ) {
        for( DataListener<T> dataListener : this.dataListeners )
            dataListener.deleted( List.of( __io( id, object ) ) );
    }

    @Override
    public void addDataListener( DataListener<T> dataListener ) {
        this.dataListeners.add( dataListener );
    }

    @Override
    public void removeDataListener( DataListener<T> dataListener ) {
        this.dataListeners.remove( dataListener );
    }

    @Override
    public Identifier<T> identifier() {
        return identifier;
    }

    @Override
    @Nonnull
    public Iterator<T> iterator() {
        return select().iterator();
    }

    @Override
    public void forEach( Consumer<? super T> action ) {
        select().forEach( action );
    }

    @Override
    public List<Metadata<T>> updatedSince( long time, int limit, int offset ) {
        return memory.selectLive()
            .mapToObj( ( id, m ) -> m )
            .filter( m -> m.modified >= time )
            .skip( offset )
            .limit( limit )
            .toList();
    }

    @Override
    public List<String> ids() {
        return memory.selectLiveIds().toList();
    }

    protected static class Memory<T> {
        private final ConcurrentMap<String, Metadata<T>> data = new ConcurrentHashMap<>();
        private final Lock lock;

        public Memory( Lock lock ) {
            this.lock = lock;
        }

        public BiStream<String, Metadata<T>> selectLive() {
            return BiStream.of( data ).filter( ( id, m ) -> !m.isDeleted() );
        }

        public BiStream<String, Metadata<T>> selectUpdatedSince( long since ) {
            return BiStream.of( data ).filter( ( id, m ) -> m.modified > since );
        }

        public Optional<Metadata<T>> get( @Nonnull String id ) {
            requireNonNull( id );
            return Optional.ofNullable( data.get( id ) );
        }

        public boolean put( @Nonnull String id, @Nonnull Metadata<T> m ) {
            requireNonNull( id );
            requireNonNull( m );
            return data.put( id, m ) == null;
        }

        public boolean put( @Nonnull String id, @Nonnull T object ) {
            requireNonNull( id );
            requireNonNull( object );
            return lock.synchronizedOn( id, () -> {
                boolean isNew = !data.containsKey( id );
                data.compute( id, ( anId, m ) -> m != null ? m.update( object ) : new Metadata<>( object ) );
                return isNew;
            } );
        }

        public Optional<Metadata<T>> remap( @Nonnull String id, @Nonnull Function<T, T> update ) {
            return lock.synchronizedOn( id, () ->
                Optional.ofNullable( data.compute( id, ( anId, m ) -> m == null
                    ? null
                    : m.update( update.apply( m.object ) ) ) ) );
        }

        public List<Pair<String, Metadata<T>>> markDeletedAll() {
            List<Pair<String, Metadata<T>>> ms = selectLive().toList();
            ms.forEach( p -> p._2.delete() );
            return ms;
        }

        public Optional<Metadata<T>> markDeleted( @Nonnull String id ) {
            return lock.synchronizedOn( id, () -> {
                Metadata<T> metadata = data.get( id );
                if( metadata != null ) {
                    metadata.delete();
                    return Optional.of( metadata );
                } else return Optional.empty();
            } );
        }

        public Optional<Metadata<T>> removePermanently( @Nonnull String id ) {
            return Optional.ofNullable( data.remove( id ) );
        }

        public Stream<String> selectLiveIds() {
            return selectLive().mapToObj( ( id, m ) -> id );
        }

    }
}
