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
import oap.util.Lists;
import oap.util.Pair;
import oap.util.Stream;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface Storage<T> extends Iterable<T> {

    Stream<T> select();

    List<T> list();

    Optional<T> get( @Nonnull String id );

    long size();

    T store( @Nonnull T object );

    void store( Collection<T> objects );

    void forEach( Consumer<? super T> action );

    Optional<T> update( @Nonnull String id, @Nonnull Function<T, T> update );

    T update( String id, @Nonnull Function<T, T> update, @Nonnull Supplier<T> init );

    Optional<T> delete( @Nonnull String id );

    void deleteAll();

    void addDataListener( DataListener<T> dataListener );

    void removeDataListener( DataListener<T> dataListener );

    Identifier<T> identifier();

    interface DataListener<D> {

        default void added( String id, D object ) {
            updated( object, true );
        }

        default void updated( String id, D object ) {
            updated( object, false );
        }

        default void deleted( String id, D object ) {
            deleted( object );
        }

        default void added( List<Pair<String, D>> objects ) {
            updated( Lists.map( objects, p -> p._2 ), true );
        }

        default void updated( List<Pair<String, D>> objects ) {
            updated( Lists.map( objects, p -> p._2 ), false );
        }

        default void deleted( List<Pair<String, D>> objects ) {
            deleted( ( Collection<D> ) Lists.map( objects, p -> p._2 ) );
        }

        /**
         * @see #updated(String, Object)
         * @see #added(String, Object)
         */
        @Deprecated
        default void updated( D object, boolean added ) {
        }

        /**
         * @see #added(List)
         * @see #updated(List)
         */
        @Deprecated
        default void updated( Collection<D> objects ) {
        }

        /**
         * @see #added(List)
         * @see #updated(List)
         */
        @Deprecated
        default void updated( Collection<D> objects, boolean added ) {
            updated( objects );
        }

        /**
         * @see #deleted(String, Object)
         */
        @Deprecated
        default void deleted( D object ) {
        }

        /**
         * @see #deleted(List)
         */
        @Deprecated
        default void deleted( Collection<D> objects ) {
            objects.forEach( this::deleted );
        }

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
