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

import oap.application.ServiceName;
import oap.concurrent.scheduler.ScheduledExecutorService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
public class AbstractPersistance<I, T> implements Closeable, AutoCloseable {

    protected final Lock lock = new ReentrantLock();
    protected final MemoryStorage<I, T> storage;
    protected final String tableName;
    protected final long delay;
    protected final Path crashDumpPath;
    @ServiceName
    public String serviceName;
    public boolean watch = false;
    protected int batchSize = 100;
    protected final ExecutorService watchExecutor = Executors.newSingleThreadExecutor();
    protected final ScheduledExecutorService scheduler = oap.concurrent.Executors.newScheduledThreadPool( 1, serviceName );
    protected volatile long lastExecuted = -1;
    protected volatile boolean stopped = false;

    public AbstractPersistance( MemoryStorage<I, T> storage, String tableName, long delay, Path crashDumpPath ) {
        this.storage = storage;
        this.tableName = tableName;
        this.delay = delay;
        this.crashDumpPath = crashDumpPath.resolve( tableName );
    }

    @Override
    public void close() throws IOException {

    }
}
