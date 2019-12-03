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

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import oap.util.MemoryMeter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleSupplier;
import java.util.function.Function;

public class StorageMetrics<I, T> {
    private final String storageName;
    private Storage<I, T> storage;
    private Map<String, Gauge<I, T>> metrics = new HashMap<>();

    public StorageMetrics( Storage<I, T> storage, String name ) {
        this.storage = storage;
        this.storageName = name;
        this.metrics.put( "oap_storage_total", new Count<>() );
        this.metrics.put( "oap_storage_memory", new Memory<>() );
    }

    public void start() {
        metrics.forEach( ( name, metric ) ->
            Metrics.gauge( name, Tags.of( "storage", storageName ), storage, storage -> metric.apply( storage ).getAsDouble() ) );
    }

    public interface Gauge<I, T> extends Function<Storage<I, T>, DoubleSupplier> {
    }

    public static class Count<I, T> implements Gauge<I, T> {
        @Override
        public DoubleSupplier apply( Storage<I, T> storage ) {
            return storage::size;
        }
    }

    public static class Memory<I, T> implements Gauge<I, T> {

        private final MemoryMeter memoryMeter;

        public Memory() {
            memoryMeter = MemoryMeter.get();
        }

        @Override
        public DoubleSupplier apply( Storage<I, T> storage ) {
            if( storage instanceof MemoryStorage<?, ?> )
                return () -> memoryMeter.measureDeep( ( ( MemoryStorage<I, T> ) storage ).memory.data );

            return () -> 0d;
        }
    }
}
