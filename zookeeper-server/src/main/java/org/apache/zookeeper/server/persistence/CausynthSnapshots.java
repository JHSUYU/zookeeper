/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.persistence;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.zookeeper.CausynthZk;
import org.apache.zookeeper.server.DataTree;

/**
 * GraphChecker: a quorum peer's snapshots as a store, in a JVM that is a GraphChecker workload
 * ({@link CausynthZk#automatic}); anywhere else it does nothing and GraphChecker is never loaded.
 *
 * <p>The store is the peer's ({@code snapshot/peer<sid>}, named where its configuration makes its
 * {@link FileTxnSnapLog}), each snapshot a version, numbered by the zxid it covers: {@link FileTxnSnapLog#save}
 * writes it, and {@link FileTxnSnapLog#restore} reads the latest one, its data tree decoded from it. So a restarted
 * life's replay, which starts after the snapshot's zxid, reads the version its previous life wrote, and that writer
 * (a learner's snapshot at NEWLEADER, say) is one of its reads' writers.</p>
 */
public final class CausynthSnapshots {

    /** The transport of a peer's snapshot store. */
    public static final String TRANSPORT = "ZK_SNAPSHOT";
    private static final Map<FileTxnSnapLog, String> STORES = Collections.synchronizedMap(new WeakHashMap<>());

    private CausynthSnapshots() {
    }

    /** {@code log} is server {@code sid}'s: its snapshots are that peer's store. */
    public static FileTxnSnapLog owned(FileTxnSnapLog log, long sid) {
        if (CausynthZk.automatic()) {
            STORES.put(log, "snapshot/peer" + sid);
        }
        return log;
    }

    /** A snapshot of {@code log}'s store is about to be written. */
    static long saving(FileTxnSnapLog log) {
        String store = STORES.get(log);
        return store == null ? -1L : MessageDoors.storing(TRANSPORT, store);
    }

    /** The snapshot covering {@code zxid} was written. */
    static void saved(FileTxnSnapLog log, long zxid) {
        String store = STORES.get(log);
        if (store != null) {
            MessageDoors.stored(TRANSPORT, store + "@" + zxid, log, bytes(zxid));
        }
    }

    /** The write {@link #saving} began is over. */
    static void ended(FileTxnSnapLog log, long write) {
        if (STORES.containsKey(log)) {
            MessageDoors.storeEnded(TRANSPORT, write);
        }
    }

    /** The latest snapshot of {@code log}'s store is about to be read. */
    static void reading(FileTxnSnapLog log) {
        String store = STORES.get(log);
        if (store != null) {
            MessageDoors.reading(TRANSPORT, store);
        }
    }

    /** The snapshot covering {@code zxid} (none: -1) was read into {@code dt}. */
    static void restored(FileTxnSnapLog log, DataTree dt, long zxid) {
        String store = STORES.get(log);
        if (store == null || zxid < 0) {
            return;
        }
        String version = store + "@" + zxid;
        MessageDoors.fetched(TRANSPORT, version, log, bytes(zxid));
        MessageDoors.decoded(TRANSPORT, version, dt);
    }

    private static byte[] bytes(long zxid) {
        return ByteBuffer.allocate(Long.BYTES).putLong(zxid).array();
    }
}
