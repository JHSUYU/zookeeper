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

package org.apache.zookeeper.test;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.DummyWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Workload for zk-buggy-710 (ZOOKEEPER-710, point zk-adhoc-02). The old client connection creates an
 * ephemeral node and writes it twice on the test thread while it owns the session. A takeover thread,
 * started before the writes and with no happens-before from them, waits TAKEOVER_DELAY_MS and then opens a
 * second handle on another server with the same session (its REVALIDATE sets the leader's session.owner to
 * that server's LearnerHandler). Natively the writes finish long before the takeover, so each reads
 * session.owner as its own server and no SessionMoved is thrown.
 *
 * GraphChecker's world orders the takeover's REVALIDATE before an old-connection write's read of
 * session.owner (the takeover's delay is the workaround that keeps that order free: container-reuse's
 * "a delay and no happens-before"). That write throws SessionMoved at SessionTrackerImpl.checkSession; with
 * ZOOKEEPER-710 put back the connection stays open, so a later write on it throws there again. Each old
 * write catches its KeeperException, which acts only after the throw. The target is the second write's throw.
 */
public class CausynthSessionMoved710Test extends ZKTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(CausynthSessionMoved710Test.class);
    public static final int CONNECTION_TIMEOUT = ClientBase.CONNECTION_TIMEOUT;
    private static final long TAKEOVER_DELAY_MS = 5000;

    private final QuorumBase qb = new QuorumBase();

    @BeforeEach
    public void setUp() throws Exception {
        qb.setUp();
    }

    @AfterEach
    public void tearDown() throws Exception {
        qb.tearDown();
    }

    @Test
    public void aMovedConnectionKeepsThrowing() throws Exception {
        String[] hostPorts = qb.hostPort.split(",");
        int leader = qb.getLeaderIndex();
        String oldServer = hostPorts[(leader + 1) % hostPorts.length];
        String newServer = hostPorts[(leader + 2) % hostPorts.length];

        ZooKeeper zk = new DisconnectableZooKeeper(oldServer, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);
        zk.create("/sm710", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        long sessionId = zk.getSessionId();
        byte[] passwd = zk.getSessionPasswd();

        AtomicReference<ZooKeeper> moved = new AtomicReference<>();
        AtomicReference<Throwable> takeoverFailure = new AtomicReference<>();
        Thread takeover = new Thread(() -> {
            try {
                Thread.sleep(TAKEOVER_DELAY_MS);
                ZooKeeper zknew = new ZooKeeper(newServer, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE, sessionId, passwd);
                moved.set(zknew);
                zknew.setData("/sm710", new byte[3], -1);
            } catch (Throwable t) {
                takeoverFailure.set(t);
            }
        }, "session-takeover");
        takeover.start();

        write(zk, 1);
        write(zk, 2);

        takeover.join();
        if (takeoverFailure.get() != null) {
            throw new AssertionError("the takeover failed", takeoverFailure.get());
        }
        moved.get().close();
        try {
            zk.close();
        } catch (Exception ignore) {
        }
    }

    private static void write(ZooKeeper zk, int n) throws InterruptedException {
        try {
            zk.setData("/sm710", new byte[n], -1);
        } catch (KeeperException e) {
            LOG.info("old-connection write {} failed: {}", n, e.code());
        }
    }

}
