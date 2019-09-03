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
import oap.util.BiStream;
import oap.util.Lists;
import oap.util.Maps;
import oap.util.Optionals;
import oap.util.Stream;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class MemoryStorage<T> implements Storage<T>, ReplicationMaster<T> {
    public final Identifier<T> identifier;
    protected final Lock lock;
    protected final List<DataListener<T>> dataListeners = new ArrayList<>();
    protected final ConcurrentMap<String, Metadata<T>> data = new ConcurrentHashMap<>();

    public MemoryStorage( Identifier<T> identifier, Lock lock ) {
        this.identifier = identifier;
        this.lock = lock;
    }

    @Override
    public Stream<T> select() {
        return Stream.of( data.values() ).filter( i -> !i.isDeleted() ).map( i -> i.object );
    }

    @Override
    public List<T> list() {
        return select().toList();
    }

    @Override
    public T store( T object ) {
        String id = identifier.getOrInit( object, this::get );
        lock.synchronizedOn( id, () -> {
            var metadata = data.get( id );

            if( metadata != null ) metadata.update( object );
            else data.computeIfAbsent( id, id1 -> new Metadata<>( object ) );
            fireUpdated( object, metadata == null );
        } );

        return object;
    }

    @Override
    public void store( Collection<T> objects ) {
        ArrayList<T> newObjects = new ArrayList<>();
        ArrayList<T> updatedObjects = new ArrayList<>();

        for( T object : objects ) {
            String id = identifier.getOrInit( object, this::get );
            lock.synchronizedOn( id, () -> {
                var metadata = data.get( id );
                if( metadata != null ) {
                    metadata.update( object );
                    updatedObjects.add( object );
                } else {
                    data.computeIfAbsent( id, id1 -> new Metadata<>( object ) );
                    newObjects.add( object );
                }
            } );
        }
        if( !newObjects.isEmpty() ) fireUpdated( newObjects, true );
        if( !updatedObjects.isEmpty() ) fireUpdated( updatedObjects, false );
    }

    @Override
    public Optional<T> update( String id, Function<T, T> update, Supplier<T> init ) {
        return updateObject( id, update, init )
            .map( m -> {
                fireUpdated( m.object, false );
                return m.object;
            } );
    }

    /**
     * @todo threadsafety of this method is questinable. Rethink it.
     */
    protected Optional<Metadata<T>> updateObject( String id, Function<T, T> update, Supplier<T> init ) {
        Metadata<T> metadata = id == null ? null : data.get( id );
        if( id == null || metadata == null )
            if( init == null ) return Optional.empty();
            else {
                T object = init.get();
                String objectId = identifier.getOrInit( object, this::get );
                return lock.synchronizedOn( objectId, () -> {
                    var m = new Metadata<>( object );
                    data.put( objectId, m );
                    return Optional.of( m );
                } );
            }
        else return lock.synchronizedOn( id, () -> {
            metadata.update( update.apply( metadata.object ) );
            return Optional.of( metadata );
        } );
    }

    @Override
    public void update( Collection<String> ids, Function<T, T> update, Supplier<T> init ) {
        fireUpdated( Stream.of( ids )
            .flatMap( id -> Optionals.toStream( updateObject( id, update, init )
                .map( m -> m.object ) ) )
            .toList(), false );
    }

    @Override
    public Optional<T> get( String id ) {
        return getLiveMetadata( id )
            .map( m -> m.object );

    }

    @Override
    public void deleteAll() {
        List<Metadata<T>> objects = selectLiveMetadatas().toList();
        objects.forEach( Metadata::delete );
        fireDeleted( Lists.map( objects, m -> m.object ) );
    }

    public Optional<T> delete( String id ) {
        return lock.synchronizedOn( id, () -> {
            Metadata<T> metadata = data.get( id );
            if( metadata != null && !metadata.isDeleted() ) {
                metadata.delete();
                fireDeleted( metadata.object );
                return Optional.of( metadata.object );
            } else return Optional.empty();
        } );
    }

    protected Optional<Metadata<T>> deleteInternalObject( String id ) {
        return Optional.ofNullable( data.remove( id ) );
    }

    protected Optional<Metadata<T>> getLiveMetadata( String id ) {
        return Maps.get( data, id )
            .filter( m -> !m.isDeleted() );
    }

    protected Stream<Metadata<T>> selectLiveMetadatas() {
        return selectLiveObjects().mapToObj( ( k, m ) -> m );
    }

    protected BiStream<String, Metadata<T>> selectLiveObjects() {
        return BiStream.of( data ).filter( ( k, m ) -> !m.isDeleted() );
    }


    @Override
    public long size() {
        return selectLiveObjects().count();
    }

    public void fireUpdated( T object, boolean isNew ) {
        for( DataListener<T> dataListener : this.dataListeners ) dataListener.updated( object, isNew );
    }

    protected void fireUpdated( Collection<T> objects, boolean isNew ) {
        if( !objects.isEmpty() )
            for( DataListener<T> dataListener : this.dataListeners )
                dataListener.updated( objects, isNew );
    }

    protected void fireDeleted( T object ) {
        for( DataListener<T> dataListener : this.dataListeners ) dataListener.deleted( object );
    }

    protected void fireDeleted( List<T> objects ) {
        if( !objects.isEmpty() )
            for( DataListener<T> dataListener : this.dataListeners )
                dataListener.deleted( objects );
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
        return selectLiveMetadatas()
            .filter( m -> m.modified >= time )
            .skip( offset )
            .limit( limit )
            .toList();
    }

    @Override
    public Collection<String> ids() {
        return selectLiveObjects()
            .mapToObj( ( k, m ) -> k )
            .toList();
    }
}
