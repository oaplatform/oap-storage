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

import oap.id.Identifier;
import oap.json.TypeIdFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static oap.storage.Storage.Lock.SERIALIZED;
import static oap.testng.Asserts.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;

public class ReplicatorTest {
    @Test
    public void masterSlave() {
        TypeIdFactory.register( Bean.class, Bean.class.getName() );
        var slave = new MemoryStorage<>( Identifier.<Bean>forId( b -> b.id ).build(), SERIALIZED );
        var master = new MemoryStorage<>( Identifier.<Bean>forId( b -> b.id ).build(), SERIALIZED );
        try( Replicator<Bean> ignored = new Replicator<>( slave, master, 50, 0 ) ) {

            var updates = new AtomicInteger();
            var addons = new AtomicInteger();
            var deletions = new AtomicInteger();
            slave.addDataListener( new Storage.DataListener<>() {
                @Override
                public void added( List<IdObject<Bean>> objects ) {
                    addons.set( objects.size() );
                }

                @Override
                public void updated( List<IdObject<Bean>> objects ) {
                    updates.set( objects.size() );
                }

                @Override
                public void deleted( List<IdObject<Bean>> objects ) {
                    deletions.set( objects.size() );
                }
            } );

            master.store( new Bean( "111" ) );
            master.store( new Bean( "222" ) );
            assertEventually( 120, 5, () -> {
                assertThat( slave.select() ).containsExactly( new Bean( "111" ), new Bean( "222" ) );
                assertThat( addons.get() ).isEqualTo( 2 );
                assertThat( updates.get() ).isEqualTo( 0 );
                assertThat( deletions.get() ).isEqualTo( 0 );
            } );

            updates.set( 0 );
            addons.set( 0 );
            master.store( new Bean( "111", "bbb" ) );
            assertEventually( 120, 5, () -> {
                assertThat( slave.select() ).containsExactly( new Bean( "111", "bbb" ), new Bean( "222" ) );
                assertThat( addons.get() ).isEqualTo( 0 );
                assertThat( updates.get() ).isEqualTo( 1 );
                assertThat( deletions.get() ).isEqualTo( 0 );
            } );

            updates.set( 0 );
            addons.set( 0 );
            master.delete( "111" );
            assertEventually( 120, 5, () -> {
                assertThat( slave.select() ).containsExactly( new Bean( "222" ) );
                assertThat( addons.get() ).isEqualTo( 0 );
                assertThat( updates.get() ).isEqualTo( 0 );
                assertThat( deletions.get() ).isEqualTo( 1 );
            } );
        }
    }
}
