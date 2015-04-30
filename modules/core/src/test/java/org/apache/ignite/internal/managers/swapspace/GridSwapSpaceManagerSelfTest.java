/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.swapspace;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;

/**
 * Tests for {@link GridSwapSpaceManager}.
 */
@SuppressWarnings({"ProhibitedExceptionThrown"})
@GridCommonTest(group = "Kernal Self")
public class GridSwapSpaceManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String SPACE_NAME = "swapspace_mgr";

    /** Partition. */
    private static final int PART = Integer.MAX_VALUE;

    /**
     *
     */
    public GridSwapSpaceManagerSelfTest() {
        super(/*start grid*/true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return c;
    }

    /**
     * Returns swap space manager instance for given Grid.
     *
     * @param ignite Grid instance.
     * @return Swap space manager.
     */
    private static GridSwapSpaceManager getSwapSpaceManager(Ignite ignite) {
        assert ignite != null;

        return ((IgniteKernal) ignite).context().swap();
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSize() throws Exception {
        final Ignite ignite = grid();

        final CountDownLatch clearCnt = new CountDownLatch(1);
        final CountDownLatch readCnt = new CountDownLatch(1);
        final CountDownLatch storeCnt = new CountDownLatch(2);
        final CountDownLatch rmvCnt = new CountDownLatch(1);

        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof SwapSpaceEvent;

                info("Received event: " + evt);

                SwapSpaceEvent e = (SwapSpaceEvent) evt;

                assert SPACE_NAME.equals(e.space());
                assert ignite.cluster().localNode().id().equals(e.node().id());

                switch (evt.type()) {
                    case EVT_SWAP_SPACE_CLEARED:
                        clearCnt.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_READ:
                        readCnt.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_STORED:
                        storeCnt.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_REMOVED:
                        rmvCnt.countDown();

                        break;

                    default:
                        assert false : "Unexpected event: " + evt;
                }

                return true;
            }
        }, EVTS_SWAPSPACE);

        GridSwapSpaceManager mgr = getSwapSpaceManager(ignite);

        ignite.getOrCreateCache((String)null);

        GridKernalContext ctx = ((IgniteKernal)ignite).context();

        GridCacheContext cctx = ((IgniteCacheProxy)ignite.cache(null)).context();

        Marshaller marsh = ctx.config().getMarshaller();

        assert mgr != null;

        // Empty data space.
        assertEquals(0, mgr.swapSize(SPACE_NAME));

        String key1 = "key1";

        String key2 = "key2";

        KeyCacheObject ckey1 = new KeyCacheObjectImpl(key1, marsh.marshal(key1));

        KeyCacheObject ckey2 = new KeyCacheObjectImpl(key2, marsh.marshal(key2));

        String val = "value";

        mgr.write(SPACE_NAME, PART, ckey1, marsh.marshal(val), cctx);

        mgr.write(SPACE_NAME, PART, ckey2, marsh.marshal(val), cctx);

        assert storeCnt.await(10, SECONDS);

        byte[] arr = mgr.read(SPACE_NAME, PART, ckey1, cctx);

        assert arr != null;

        assert val.equals(marsh.unmarshal(arr, cctx.deploy().globalLoader()));

        final GridTuple<Boolean> b = F.t(false);

        mgr.remove(SPACE_NAME, PART, ckey1, cctx, new CI1<byte[]>() {
            @Override public void apply(byte[] rmv) {
                b.set(rmv != null);
            }
        });

        assert b.get();

        assert rmvCnt.await(10, SECONDS);
        assert readCnt.await(10, SECONDS);

        mgr.clear(SPACE_NAME);

        assert clearCnt.await(10, SECONDS) : "Count: " + clearCnt.getCount();
    }
}
