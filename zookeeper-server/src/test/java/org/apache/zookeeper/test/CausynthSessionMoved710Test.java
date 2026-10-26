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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.DummyWatcher;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Workload for zk-buggy-710 (ZOOKEEPER-710, point zk-adhoc-02). The old client connection owns the
 * session and writes while it owns it; then a second handle on another server takes the session over
 * (its late REVALIDATE sets the leader's session.owner to the new server's LearnerHandler). The
 * recording keeps every old-connection write ahead of that REVALIDATE, so each reads session.owner as
 * its own server and succeeds -- no SessionMoved is thrown natively.
 *
 * GraphChecker's world re-pairs the two old-connection setData reads of session.owner to the late
 * REVALIDATE's setOwner: the first throws SessionMoved at SessionTrackerImpl.checkSession and, because
 * ZOOKEEPER-710 is put back (FinalRequestProcessor does not tear the connection down), the connection
 * stays open and the second setData throws SessionMoved there again -- the permanent SESSIONMOVED. The
 * second throw is the target; on the fixed tree the first throw closes the connection so the second
 * request never reaches checkSession.
 */
public class CausynthSessionMoved710Test extends ZKTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(CausynthSessionMoved710Test.class);
    public static final int CONNECTION_TIMEOUT = ClientBase.CONNECTION_TIMEOUT;

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
        int oldServer = (leader + 1) % hostPorts.length;
        int newServer = (leader + 2) % hostPorts.length;

        DisconnectableZooKeeper zk = new DisconnectableZooKeeper(
            hostPorts[oldServer], CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);
        zk.create("/sm710", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zk.setData("/sm710", new byte[1], -1);
        zk.setData("/sm710", new byte[2], -1);
        LOG.info("old connection on {} wrote while it owned the session", hostPorts[oldServer]);

        zk.dontReconnect();
        ZooKeeper zknew = new ZooKeeper(
            hostPorts[newServer], CONNECTION_TIMEOUT, DummyWatcher.INSTANCE,
            zk.getSessionId(), zk.getSessionPasswd());
        zknew.setData("/sm710", new byte[3], -1);
        LOG.info("session moved to {}", hostPorts[newServer]);

        zknew.close();
        try {
            zk.close();
        } catch (Exception ignore) {
        }
    }

}
