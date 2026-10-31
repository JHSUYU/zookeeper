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
 * Workload for zk-inj-04 (point zk-adhoc-01), from CausynthSessionMoved710Test. A client on a follower
 * creates a node, then writes it on a thread of its own while a closer thread, started before the write and
 * with no happens-before from it, waits CLOSE_DELAY_MS and closes the client. Natively the write is served
 * long before the close, so its checkSession at the leader reads the session as open.
 *
 * The write's forwarded request reaches the leader's PrepRequestProcessor before the close's, as the client
 * sent them; the case's world is the close's arrival at the leader (the follower's LearnerHandler) before
 * the write's checkSession there. The write runs on a thread of its own, so its client request is rooted at
 * that thread's start, a point a confirmation replay can hold (CausynthSessionMoved710Test's second write).
 * The write catches its KeeperException, which acts only after the throw.
 */
public class CausynthSessionCloseOrderTest extends ZKTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(CausynthSessionCloseOrderTest.class);
    public static final int CONNECTION_TIMEOUT = ClientBase.CONNECTION_TIMEOUT;
    private static final long CLOSE_DELAY_MS = 2000;

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
    public void aWriteSentBeforeTheClose() throws Exception {
        String[] hostPorts = qb.hostPort.split(",");
        int leader = qb.getLeaderIndex();
        String follower = hostPorts[(leader + 1) % hostPorts.length];

        ZooKeeper zk = new ZooKeeper(follower, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);
        zk.create("/sc", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                Thread.sleep(CLOSE_DELAY_MS);
                zk.close();
            } catch (Throwable t) {
                closeFailure.set(t);
            }
        }, "session-closer");
        closer.start();

        Thread writer = new Thread(() -> {
            try {
                zk.setData("/sc", new byte[1], -1);
            } catch (KeeperException e) {
                LOG.info("the write before the close failed: {}", e.code());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "session-writer");
        writer.start();
        writer.join();

        closer.join();
        if (closeFailure.get() != null) {
            throw new AssertionError("the close failed", closeFailure.get());
        }
    }
}
