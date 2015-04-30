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

package org.apache.ignite.internal.processors.offheap;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.offheap.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.util.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Manages offheap memory caches.
 */
public class GridOffHeapProcessor extends GridProcessorAdapter {
    /** */
    private final ConcurrentHashMap8<String, GridOffHeapPartitionedMap> offheap =
        new ConcurrentHashMap8<>();

    /** */
    private final Marshaller marsh;

    /**
     * @param ctx Kernal context.
     */
    public GridOffHeapProcessor(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.config().getMarshaller();
    }

    /**
     * Creates offheap map for given space name. Previous one will be destructed if it exists.
     *
     * @param spaceName Space name.
     * @param parts Partitions number.
     * @param init Initial size.
     * @param max Maximum size.
     * @param lsnr Eviction listener.
     */
    public void create(@Nullable String spaceName, int parts, long init, long max,
        @Nullable GridOffHeapEvictListener lsnr) {
        spaceName = maskNull(spaceName);

        GridOffHeapPartitionedMap m = GridOffHeapMapFactory.unsafePartitionedMap(parts, 1024, 0.75f, init, max,
            (short)512, lsnr);

        GridOffHeapPartitionedMap old = offheap.put(spaceName, m);

        if (old != null)
            old.destruct();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        for (GridOffHeapPartitionedMap m : offheap.values())
            m.destruct();
    }

    /**
     * Gets offheap swap space for given space name.
     *
     * @param spaceName Space name.
     * @return Offheap swap space.
     */
    @SuppressWarnings("unchecked")
    @Nullable private GridOffHeapPartitionedMap offheap(@Nullable String spaceName) {
        return offheap.get(maskNull(spaceName));
    }

    /**
     * Ensures that we have {@code keyBytes}.
     *
     * @param key Key.
     * @param cctx Cache context.
     * @return Key bytes.
     * @throws IgniteCheckedException If failed.
     */
    private byte[] keyBytes(KeyCacheObject key, GridCacheContext cctx) throws IgniteCheckedException {
        assert key != null;

        byte[] keyBytes = key.valueBytes(cctx.cacheObjectContext());

        return keyBytes != null ? keyBytes : marsh.marshal(key);
    }

    /**
     * Masks {@code null} space name.
     *
     * @param spaceName Space name.
     * @return Masked space name.
     */
    private String maskNull(@Nullable String spaceName) {
        if (spaceName == null)
            return "gg-dflt-offheap-swap";

        return spaceName;
    }

    /**
     * Checks if offheap space contains value for the given key.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @return {@code True} If offheap space contains value for the given key.
     * @throws IgniteCheckedException If failed.
     */
    public boolean contains(@Nullable String spaceName, int part, KeyCacheObject key, GridCacheContext cctx)
        throws IgniteCheckedException {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        boolean hit = m != null && m.contains(part, U.hash(key), keyBytes(key, cctx));

        if (cctx.config().isStatisticsEnabled())
            cctx.cache().metrics0().onOffHeapRead(hit);

        return hit;
    }

    /**
     * Gets value bytes from offheap space for the given key.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public byte[] get(@Nullable String spaceName, int part, KeyCacheObject key, GridCacheContext cctx)
        throws IgniteCheckedException {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        byte[] bytes = m == null ? null : m.get(part, U.hash(key), keyBytes(key, cctx));

        if (cctx.config().isStatisticsEnabled())
            cctx.cache().metrics0().onOffHeapRead(bytes != null);

        return bytes;
    }

    /**
     * Gets value pointer from offheap space for the given key. While pointer is in use eviction is
     * disabled for corresponding entry. Eviction for entry is enabled when {@link #put} or
     * {@link #enableEviction} is called.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @return Tuple where first value is pointer and second is value size.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgniteBiTuple<Long, Integer> valuePointer(@Nullable String spaceName, int part, KeyCacheObject key,
        GridCacheContext cctx) throws IgniteCheckedException {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        return m == null ? null : m.valuePointer(part, U.hash(key), keyBytes(key, cctx));
    }

    /**
     * Enables eviction for entry after {@link #valuePointer} was called.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void enableEviction(@Nullable String spaceName, int part, KeyCacheObject key, GridCacheContext cctx)
        throws IgniteCheckedException {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        if (m != null)
            m.enableEviction(part, U.hash(key), keyBytes(key, cctx));
    }

    /**
     * Gets value from offheap space for the given key.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @param ldr Class loader.  @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> T getValue(@Nullable String spaceName, int part, KeyCacheObject key,
        GridCacheContext cctx, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        byte[] valBytes = get(spaceName, part, key, cctx);

        if (valBytes == null)
            return null;

        return marsh.unmarshal(valBytes, ldr == null ? U.gridClassLoader() : ldr);
    }

    /**
     * Removes value from offheap space for the given key.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public byte[] remove(@Nullable String spaceName, int part, KeyCacheObject key, GridCacheContext cctx)
        throws IgniteCheckedException {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        if (m == null)
            return null;

        byte[] bytes = m.remove(part, U.hash(key), keyBytes(key, cctx));

        if(bytes != null && cctx.config().isStatisticsEnabled())
            cctx.cache().metrics0().onOffHeapRemove();

        return bytes;
    }

    /**
     * Puts the given value to offheap space for the given key.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param valBytes Value bytes.
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void put(@Nullable String spaceName, int part, KeyCacheObject key, byte[] valBytes, GridCacheContext cctx)
        throws IgniteCheckedException {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        if (m == null)
            throw new IgniteCheckedException("Failed to write data to off-heap space, no space registered for name: " +
                spaceName);

        m.put(part, U.hash(key), keyBytes(key, cctx), valBytes);

        if (cctx.config().isStatisticsEnabled())
            cctx.cache().metrics0().onOffHeapWrite();

        if (cctx.events().isRecordable(EVT_CACHE_OBJECT_TO_OFFHEAP))
            cctx.events().addEvent(part, key, cctx.nodeId(), (IgniteUuid)null, null,
                EVT_CACHE_OBJECT_TO_OFFHEAP, null, false, null, true, null, null, null);
    }

    /**
     * Removes value from offheap space for the given key.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @param key Key.
     * @param cctx Cache context.
     * @return {@code True} If succeeded.
     * @throws IgniteCheckedException If failed.
     */
    public boolean removex(@Nullable String spaceName, int part, KeyCacheObject key, GridCacheContext cctx)
        throws IgniteCheckedException {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        if (m == null)
            return false;

        boolean rmv = m.removex(part, U.hash(key), keyBytes(key, cctx));

        if(rmv && cctx.config().isStatisticsEnabled())
            cctx.cache().metrics0().onOffHeapRemove();

        return rmv;
    }

    /**
     * Gets iterator over contents of the given space.
     *
     * @param spaceName Space name.
     * @return Iterator.
     */
    public GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> iterator(@Nullable String spaceName) {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        return m == null ? new GridEmptyCloseableIterator<IgniteBiTuple<byte[], byte[]>>() : m.iterator();
    }

    /**
     * Gets iterator over contents of the given space.
     *
     * @param spaceName Space name.
     * @param c Key/value closure.
     * @return Iterator.
     */
    public <T> GridCloseableIterator<T> iterator(@Nullable String spaceName,
        CX2<T2<Long, Integer>, T2<Long, Integer>, T> c) {
        assert c != null;

        GridOffHeapPartitionedMap m = offheap(spaceName);

        return m == null ? new GridEmptyCloseableIterator<T>() : m.iterator(c);
    }

    /**
     * Gets iterator over contents of the given space.
     *
     * @param spaceName Space name.
     * @param c Key/value closure.
     * @param part Partition.
     * @return Iterator.
     */
    public <T> GridCloseableIterator<T> iterator(@Nullable String spaceName,
        CX2<T2<Long, Integer>, T2<Long, Integer>, T> c, int part) {
        assert c != null;

        GridOffHeapPartitionedMap m = offheap(spaceName);

        return m == null ? new GridEmptyCloseableIterator<T>() : m.iterator(c, part);
    }

    /**
     * Gets number of elements in the given space.
     *
     * @param spaceName Space name. Optional.
     * @return Number of elements or {@code -1} if no space with the given name has been found.
     */
    public long entriesCount(@Nullable String spaceName) {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        return m == null ? -1 : m.size();
    }

    /**
     * Gets number of elements in the given space.
     *
     * @param spaceName Space name. Optional.
     * @param parts Partitions.
     * @return Number of elements or {@code -1} if no space with the given name has been found.
     */
    public long entriesCount(@Nullable String spaceName, Set<Integer> parts) {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        return m == null ? -1 : m.size(parts);
    }

    /**
     * Gets size of a memory allocated for the entries of the given space.
     *
     * @param spaceName Space name. Optional.
     * @return Allocated memory size or {@code -1} if no space with the given name has been found.
     */
    public long allocatedSize(@Nullable String spaceName) {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        return m == null ? -1 : m.allocatedSize();
    }

    /**
     * Gets iterator over contents of partition.
     *
     * @param spaceName Space name.
     * @param part Partition.
     * @return Iterator.
     */
    public GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> iterator(@Nullable String spaceName, int part) {
        GridOffHeapPartitionedMap m = offheap(spaceName);

        return m == null ? new GridEmptyCloseableIterator<IgniteBiTuple<byte[], byte[]>>() : m.iterator(part);
    }
}
