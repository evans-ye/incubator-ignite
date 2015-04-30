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
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.swapspace.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.events.EventType.*;

/**
 *
 */
@SkipDaemon
public class GridSwapSpaceManager extends GridManagerAdapter<SwapSpaceSpi> {
    /** */
    private Marshaller marsh;

    /**
     * @param ctx Grid kernal context.
     */
    public GridSwapSpaceManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getSwapSpaceSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        getSpi().setListener(new SwapSpaceSpiListener() {
            @Override public void onSwapEvent(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes,
                @Nullable byte[] valBytes) {
                if (ctx.event().isRecordable(evtType)) {
                    String msg = null;

                    switch (evtType) {
                        case EVT_SWAP_SPACE_DATA_READ: {
                            msg = "Swap space data read [space=" + spaceName + ']';

                            break;
                        }

                        case EVT_SWAP_SPACE_DATA_STORED: {
                            msg = "Swap space data stored [space=" + spaceName + ']';

                            break;
                        }

                        case EVT_SWAP_SPACE_DATA_REMOVED: {
                            msg = "Swap space data removed [space=" + spaceName + ']';

                            break;
                        }

                        case EVT_SWAP_SPACE_CLEARED: {
                            msg = "Swap space cleared [space=" + spaceName + ']';

                            break;
                        }

                        case EVT_SWAP_SPACE_DATA_EVICTED: {
                            msg = "Swap entry evicted [space=" + spaceName + ']';

                            break;
                        }

                        default: {
                            assert false : "Unknown event type: " + evtType;
                        }
                    }

                    ctx.event().record(new SwapSpaceEvent(ctx.discovery().localNode(), msg, evtType, spaceName));
                }

                // Always notify grid cache processor.
                if (evtType == EVT_SWAP_SPACE_DATA_EVICTED && spaceName != null) {
                    assert keyBytes != null;
                    assert valBytes != null;

                    // Cache cannot use default swap space.
                    ctx.cache().onEvictFromSwap(spaceName, keyBytes, valBytes);
                }
            }
        });

        startSpi();

        marsh = ctx.config().getMarshaller();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        getSpi().setListener(null);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Reads value from swap.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public byte[] read(@Nullable String spaceName, int part, KeyCacheObject key, GridCacheContext cctx)
        throws IgniteCheckedException {
        assert key != null;

        try {
            byte[] bytes = getSpi().read(spaceName, new SwapKey(key.value(cctx.cacheObjectContext(), false), part,
                key.valueBytes(cctx.cacheObjectContext())), context(cctx.deploy().globalLoader()));

            if (cctx.config().isStatisticsEnabled())
                cctx.cache().metrics0().onSwapRead(bytes != null);

            return bytes;
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to read from swap space [space=" + spaceName +
                ", key=" + key + ']', e);
        }
    }

    /**
     * Reads value from swap.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <T> T readValue(@Nullable String spaceName, int part, KeyCacheObject key, GridCacheContext cctx)
        throws IgniteCheckedException {
        assert key != null;

        return unmarshal(read(spaceName, part, key, cctx), cctx.deploy().globalLoader());
    }

    /**
     * Writes value to swap.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param val Value.
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void write(@Nullable String spaceName, int part, KeyCacheObject key, byte[] val, GridCacheContext cctx)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        try {
            SwapKey swapKey = new SwapKey(key.value(cctx.cacheObjectContext(), false), part,
                key.valueBytes(cctx.cacheObjectContext()));

            getSpi().store(spaceName, swapKey, val, context(cctx.deploy().globalLoader()));

            if (cctx.config().isStatisticsEnabled())
                cctx.cache().metrics0().onSwapWrite();
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to write to swap space [space=" + spaceName + ", key=" + key +
                ", valLen=" + val.length + ']', e);
        }
    }

    /**
     * Writes batch to swap.
     *
     * @param spaceName Space name.
     * @param swapped Swapped entries.
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void writeAll(String spaceName, Iterable<GridCacheBatchSwapEntry> swapped, GridCacheContext cctx)
        throws IgniteCheckedException {
        Map<SwapKey, byte[]> batch = new LinkedHashMap<>();

        int cnt = 0;

        for (GridCacheBatchSwapEntry entry : swapped) {
            SwapKey swapKey = new SwapKey(entry.key().value(cctx.cacheObjectContext(), false),
                entry.partition(),
                entry.key().valueBytes(cctx.cacheObjectContext()));

            batch.put(swapKey, entry.marshal());

            cnt++;
        }

        getSpi().storeAll(spaceName, batch, context(cctx.deploy().globalLoader()));

        if (cctx.config().isStatisticsEnabled())
            cctx.cache().metrics0().onSwapWrite(cnt);
    }

    /**
     * Writes value to swap.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param val Value.
     * @param cctx Cache context.
     * */
    public void write(@Nullable String spaceName, int part, KeyCacheObject key, @Nullable Object val,
        GridCacheContext cctx) throws IgniteCheckedException {
        assert key != null;

        write(spaceName, part, key, marshal(val), cctx);
    }

    /**
     * Removes value from swap.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @param c Optional closure that takes removed value and executes after actual
     *      removing. If there was no value in storage the closure is executed given
     *      {@code null} value as parameter.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(@Nullable String spaceName, int part, KeyCacheObject key,
        final GridCacheContext cctx, @Nullable final IgniteInClosure<byte[]> c) throws IgniteCheckedException {
        assert key != null;

        try {
            SwapKey swapKey = new SwapKey(key.value(cctx.cacheObjectContext(), false), part,
                key.valueBytes(cctx.cacheObjectContext()));

            getSpi().remove(spaceName, swapKey, new IgniteInClosure<byte[]>() {
                @Override public void apply(byte[] bytes) {
                    if (c != null)
                        c.apply(bytes);

                    if (bytes != null && cctx.config().isStatisticsEnabled())
                        cctx.cache().metrics0().onSwapRemove();
                }
            }, context(cctx.deploy().globalLoader()));
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to remove from swap space [space=" + spaceName +
                ", key=" + key + ']', e);
        }
    }

    /**
     * Removes value from swap.
     *
     * @param spaceName Space name.
     * @param keys Keys.
     * @param cctx Cache context.
     * @param c Optional closure that takes removed value and executes after actual
     *      removing. If there was no value in storage the closure is executed given
     *      {@code null} value as parameter.
     * @throws IgniteCheckedException If failed.
     */
    public void removeAll(@Nullable String spaceName, Collection<KeyCacheObject> keys, GridCacheContext cctx,
        IgniteBiInClosure<SwapKey, byte[]> c) throws IgniteCheckedException {
        assert keys != null;

        Collection<SwapKey> swapKeys = new ArrayList<>(keys.size());

        for (KeyCacheObject key : keys) {
            SwapKey swapKey = new SwapKey(key.value(cctx.cacheObjectContext(), false),
                cctx.affinity().partition(key),
                key.valueBytes(cctx.cacheObjectContext()));

            swapKeys.add(swapKey);
        }

        try {
            getSpi().removeAll(spaceName, swapKeys, c, context(cctx.deploy().globalLoader()));

            if (cctx.config().isStatisticsEnabled())
                cctx.cache().metrics0().onSwapRemove();
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to remove from swap space [space=" + spaceName + ", " +
                "keysCnt=" + keys.size() + ']', e);
        }
    }

    /**
     * Gets size in bytes for swap space.
     *
     * @param spaceName Space name.
     * @return Swap size.
     * @throws IgniteCheckedException If failed.
     */
    public long swapSize(@Nullable String spaceName) throws IgniteCheckedException {
        try {
            return getSpi().size(spaceName);
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to get swap size for space: " + spaceName, e);
        }
    }

    /**
     * Gets number of swap entries (keys).
     *
     * @param spaceName Space name.
     * @return Number of stored entries in swap space.
     * @throws IgniteCheckedException If failed.
     */
    public long swapKeys(@Nullable String spaceName) throws IgniteCheckedException {
        try {
            return getSpi().count(spaceName);
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to get swap keys count for space: " + spaceName, e);
        }
    }

    /**
     * Gets number of swap entries for given partitions.
     *
     * @param spaceName Space name.
     * @param parts Partitions.
     * @return Number of swap entries for given partitions.
     * @throws IgniteCheckedException If failed.
     */
    public long swapKeys(@Nullable String spaceName, Set<Integer> parts) throws IgniteCheckedException {
        try {
            return getSpi().count(spaceName, parts);
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to get swap keys count for space: " + spaceName, e);
        }
    }

    /**
     * @param spaceName Space name.
     * @throws IgniteCheckedException If failed.
     */
    public void clear(@Nullable String spaceName) throws IgniteCheckedException {
        try {
            getSpi().clear(spaceName);
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to clear swap space [space=" + spaceName + ']', e);
        }
    }

    /**
     * Gets iterator over space entries.
     *
     * @param spaceName Space name.
     * @return Iterator over space entries or {@code null} if space is unknown.
     * @throws IgniteSpiException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName)
        throws IgniteCheckedException {
        try {
            IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> it = getSpi().rawIterator(spaceName);

            return it == null ? null : new GridSpiCloseableIteratorWrapper<>(it);
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to get iterator over swap space [space=" + spaceName + ']', e);
        }
    }

    /**
     * Gets raw iterator over space entries.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @return Iterator over space entries or {@code null} if space is unknown.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName, int part)
        throws IgniteCheckedException{
        try {
            IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> it = getSpi().rawIterator(spaceName, part);

            return it == null ? new GridEmptyCloseableIterator<Map.Entry<byte[], byte[]>>() :
                new GridSpiCloseableIteratorWrapper<>(it);
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to get iterator over swap space [space=" + spaceName + ']', e);
        }
    }

    /**
     * Gets iterator over space entries.
     *
     * @param spaceName Space name.
     * @param ldr Class loader.
     * @return Iterator over space entries or {@code null} if space is unknown.
     * @throws IgniteSpiException If failed.
     */
    @Nullable public <K> GridCloseableIterator<K> keysIterator(@Nullable String spaceName,
        @Nullable ClassLoader ldr) throws IgniteCheckedException {
        try {
            IgniteSpiCloseableIterator<K> it = getSpi().keyIterator(spaceName, context(ldr));

            return it == null ? null : new GridSpiCloseableIteratorWrapper<>(it);
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to get iterator over swap space [space=" + spaceName + ']', e);
        }
    }

    /**
     * @param swapBytes Swap bytes to unmarshal.
     * @param ldr Class loader.
     * @return Unmarshalled value.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T unmarshal(byte[] swapBytes, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        if (swapBytes == null)
            return null;

        return marsh.unmarshal(swapBytes, ldr != null ? ldr : U.gridClassLoader());
    }

    /**
     * Marshals object.
     *
     * @param obj Object to marshal.
     * @return Marshalled array.
     * @throws IgniteCheckedException If failed.
     */
    private byte[] marshal(Object obj) throws IgniteCheckedException {
        return ctx.config().getMarshaller().marshal(obj);
    }

    /**
     * @param clsLdr Class loader.
     * @return Swap context.
     */
    private SwapContext context(@Nullable ClassLoader clsLdr) {
        SwapContext ctx = new SwapContext();

        ctx.classLoader(clsLdr != null ? clsLdr : U.gridClassLoader());

        return ctx;
    }
}
