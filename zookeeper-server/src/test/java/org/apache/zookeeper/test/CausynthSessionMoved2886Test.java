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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.DummyWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Workload for zk-buggy-2886 (ZOOKEEPER-2886, point zk-adhoc-02), the multi-only shape of ZOOKEEPER-710. The
 * old client connection issues only multi ops on the test thread while it owns the session: a create, then
 * two setData. A takeover thread, started before them and with no happens-before from them, waits
 * TAKEOVER_DELAY_MS and then opens a second handle on another server with the same session (its REVALIDATE
 * sets the leader's session.owner to that server's LearnerHandler). Natively the multis finish long before the
 * takeover, so each reads session.owner as its own server and no SessionMoved is thrown.
 *
 * GraphChecker's world orders the takeover's REVALIDATE before an old-connection multi's read of
 * session.owner (the takeover's delay is the workaround that keeps that order free: container-reuse's
 * "a delay and no happens-before"). That multi's sub-op throws SessionMoved at SessionTrackerImpl.checkSession;
 * with ZOOKEEPER-2886 put back it stays a per-op ErrorResult, the connection stays open, and a later multi on
 * it throws there again. Each old multi catches its KeeperException, which acts only after the throw. The
 * target is the second setData multi's throw.
 *
 * Neither handle is closed at the end: a close sends closeSession, whose setSessionClosing is a second in-window
 * writer of the session's isClosing, which the target's checkSession also reads (the servers' shutdown ends both).
 */
public class CausynthSessionMoved2886Test extends ZKTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(CausynthSessionMoved2886Test.class);
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
    public void aMovedMultiOnlyConnectionKeepsThrowing() throws Exception {
        String[] hostPorts = qb.hostPort.split(",");
        int leader = qb.getLeaderIndex();
        String oldServer = hostPorts[(leader + 1) % hostPorts.length];
        String newServer = hostPorts[(leader + 2) % hostPorts.length];

        ZooKeeper zk = new DisconnectableZooKeeper(oldServer, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);
        zk.multi(Arrays.asList(Op.create("/sm2886", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)));
        long sessionId = zk.getSessionId();
        byte[] passwd = zk.getSessionPasswd();

        AtomicReference<Throwable> takeoverFailure = new AtomicReference<>();
        Thread takeover = new Thread(() -> {
            try {
                Thread.sleep(TAKEOVER_DELAY_MS);
                ZooKeeper zknew = new ZooKeeper(newServer, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE, sessionId, passwd);
                zknew.multi(Arrays.asList(Op.setData("/sm2886", new byte[3], -1)));
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
    }

    private static void write(ZooKeeper zk, int n) throws InterruptedException {
        try {
            zk.multi(Arrays.asList(Op.setData("/sm2886", new byte[n], -1)));
        } catch (KeeperException e) {
            LOG.info("old-connection multi {} failed: {}", n, e.code());
        }
    }

}
