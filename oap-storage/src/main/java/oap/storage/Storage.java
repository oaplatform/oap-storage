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

import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.concurrent.Threads;
import oap.id.Identifier;
import oap.util.Stream;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface Storage<I, T> extends Iterable<T> {

    Stream<T> select();

    List<T> list();

    Optional<T> get( @Nonnull I id );

    T get( I id, @Nonnull Supplier<T> init );

    long size();

    T store( @Nonnull T object );

    void store( Collection<T> objects );

    void forEach( Consumer<? super T> action );

    Optional<T> update( @Nonnull I id, @Nonnull Function<T, T> update );

    T update( I id, @Nonnull Function<T, T> update, @Nonnull Supplier<T> init );

    Optional<T> delete( @Nonnull I id );

    void deleteAll();

    void addDataListener( DataListener<I, T> dataListener );

    void removeDataListener( DataListener<I, T> dataListener );

    Identifier<I, T> identifier();

    /**
     * The method is called once per replication period, all changes there are gathered.
     * @param added objects added to slave
     * @param updated objects updated in slave
     * @param deleted objects deleted from slave
     * @param <I> key type
     * @param <T> value type
     */
    <I, T> void fireChanged( List<DataListener.IdObject<I, T>> added,
                             List<DataListener.IdObject<I, T>> updated,
                             List<DataListener.IdObject<I, T>> deleted );

    interface DataListener<DI, D> {
        default void added( List<IdObject<DI, D>> objects ) {}

        default void updated( List<IdObject<DI, D>> objects ) {}

        default void deleted( List<IdObject<DI, D>> objects ) {}

        @ToString
        @EqualsAndHashCode
        class IdObject<DI, D> implements Serializable {
            @Serial
            private static final long serialVersionUID = -5793630001926149000L;

            public final DI id;
            public final D object;

            public IdObject( DI id, D object ) {
                this.id = id;
                this.object = object;
            }

            @SuppressWarnings( "checkstyle:MethodName" )
            public static <DI, D> IdObject<DI, D> __io( DI id, D object ) {
                return new IdObject<>( id, object );
            }

            public final DI id() {
                return id;
            }

            public final D object() {
                return object;
            }
        }
    }

    interface Lock {
        Lock CONCURRENT = new ConcurrentLock();
        Lock SERIALIZED = new SerializedLock();

        void synchronizedOn( Object id, Runnable run );

        <R> R synchronizedOn( Object id, Supplier<R> run );

        class ConcurrentLock implements Lock {
            @Override
            public final void synchronizedOn( Object id, Runnable run ) {
                run.run();
            }

            @Override
            public final <R> R synchronizedOn( Object id, Supplier<R> run ) {
                return run.get();
            }
        }

        class SerializedLock implements Lock {
            @Override
            public final void synchronizedOn( Object id, Runnable run ) {
                Threads.synchronizedOn( id, run );
            }

            @Override
            public final <R> R synchronizedOn( Object id, Supplier<R> run ) {
                return Threads.synchronizedOn( id, run );
            }
        }
    }
}
