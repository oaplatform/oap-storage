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

import oap.metrics.Metrics;
import oap.util.MemoryMeter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class StorageMetrics<T> {
    private Storage<T> storage;
    private Map<String, Gauge<T>> metrics = new HashMap<>();

    public StorageMetrics( Storage<T> storage, String name ) {
        this.storage = storage;
        this.metrics.put( name + ".total", new Count<>() );
        this.metrics.put( name + ".memory", new Memory<>() );
    }

    public void start() {
        metrics.forEach( ( name, metric ) -> Metrics.measureGauge( name, () -> metric.apply( storage ) ) );
    }

    public interface Gauge<T> extends Function<Storage<T>, Supplier<Long>> {
    }

    public static class Count<T> implements Gauge<T> {
        @Override
        public Supplier<Long> apply( Storage<T> storage ) {
            return storage::size;
        }
    }

    public static class Memory<T> implements Gauge<T> {

        private final MemoryMeter memoryMeter;

        public Memory() {
            memoryMeter = MemoryMeter.get();
        }

        @Override
        public Supplier<Long> apply( Storage<T> storage ) {
            if( storage instanceof MemoryStorage<?> ) {
                return () -> memoryMeter.measureDeep( ( ( MemoryStorage<T> ) storage ).memory.data );
            }

            return () -> 0L;
        }
    }
}