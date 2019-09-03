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

import oap.concurrent.Threads;
import oap.util.Stream;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface Storage<T> extends Iterable<T> {

    Stream<T> select();

    List<T> list();

    Optional<T> get( String id );

    long size();

    T store( T object );

    void store( Collection<T> objects );

    void forEach( Consumer<? super T> action );

    default Optional<T> update( String id, Function<T, T> update ) {
        return update( id, update, null );
    }

    Optional<T> update( String id, Function<T, T> update, Supplier<T> init );

    void update( Collection<String> ids, Function<T, T> update, Supplier<T> init );

    default void update( Collection<String> ids, Function<T, T> update ) {
        update( ids, update, null );
    }

    Optional<T> delete( String id );

    void deleteAll();

    void addDataListener( DataListener<T> dataListener );

    void removeDataListener( DataListener<T> dataListener );

    Identifier<T> identifier();

    interface DataListener<D> {

        default void updated( D object, boolean added ) {
        }

        default void updated( Collection<D> objects ) {
        }

        default void updated( Collection<D> objects, boolean added ) {
            updated( objects );

            objects.forEach( obj -> updated( obj, added ) );
        }


        default void deleted( D object ) {
        }

        default void deleted( Collection<D> objects ) {
            objects.forEach( this::deleted );
        }

        @Deprecated
        default void fsync() {}
    }

    interface Lock {
        Lock CONCURRENT = new ConcurrentLock();
        Lock SERIALIZED = new SerializedLock();

        void synchronizedOn( String id, Runnable run );

        <R> R synchronizedOn( String id, Supplier<R> run );

        final class ConcurrentLock implements Lock {
            @Override
            public final void synchronizedOn( String id, Runnable run ) {
                run.run();
            }

            @Override
            public final <R> R synchronizedOn( String id, Supplier<R> run ) {
                return run.get();
            }
        }

        final class SerializedLock implements Lock {
            @Override
            public final void synchronizedOn( String id, Runnable run ) {
                Threads.synchronizedOn( id, run );
            }

            @Override
            public final <R> R synchronizedOn( String id, Supplier<R> run ) {
                return Threads.synchronizedOn( id, run );
            }
        }
    }
}
