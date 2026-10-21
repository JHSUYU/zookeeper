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

package org.apache.zookeeper.server.quorum;

import static org.apache.zookeeper.server.quorum.CausynthQuorumHarness.await;
import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * GraphChecker workload of ZOOKEEPER-5094 (apache/zookeeper PR 2459). A follower F logs a create that its leader has
 * not committed, restarts, and is told to TRUNC; the create commits while F's sync is between TRUNC and NEWLEADER, so
 * the leader sends it as PROPOSAL+COMMIT before NEWLEADER and F applies it in memory only. NEWLEADER's snapshot covers
 * it, and F's next restart replays snapshot plus log. Natively everything passes; a sync that ends before NEWLEADER
 * leaves F's database ahead of its log, and that restart throws at ZKDatabase.addCommittedProposal.
 */
public class CausynthTruncSyncTest extends QuorumPeerTestBase {

    private static final int PARTICIPANTS = 3;

    private final CausynthQuorumHarness harness = new CausynthQuorumHarness();
    private final MainThread[] peers = new MainThread[PARTICIPANTS + 1];
    private final int[] clientPorts = new int[PARTICIPANTS + 1];
    private ZooKeeper client;

    @AfterEach
    public void tearDown() throws Exception {
        harness.abort();
        if (client != null) {
            client.close();
        }
        for (int sid = PARTICIPANTS; sid >= 1; sid--) {
            if (peers[sid] != null) {
                peers[sid].shutdown();
            }
        }
    }

    @Test
    @Timeout(value = 120)
    public void aTruncSyncedFollowerRestarts() throws Exception {
        ClientBase.setupTestEnv();
        StringBuilder quorumCfg = new StringBuilder();
        for (int sid = 1; sid <= PARTICIPANTS; sid++) {
            clientPorts[sid] = PortAssignment.unique();
            quorumCfg.append(String.format("server.%d=127.0.0.1:%d:%d:participant;127.0.0.1:%d%n",
                sid, PortAssignment.unique(), PortAssignment.unique(), clientPorts[sid]));
        }
        for (int sid = 1; sid <= PARTICIPANTS; sid++) {
            peers[sid] = harness.participant(sid, clientPorts[sid], quorumCfg.toString(),
                "snapCount=100000\nadmin.enableServer=false\n");
            peers[sid].start();
        }
        for (int sid = 1; sid <= PARTICIPANTS; sid++) {
            assertTrue(ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[sid], CONNECTION_TIMEOUT),
                "participant " + sid + " did not start");
        }
        int leader = role(true);
        int follower = role(false);

        client = ClientBase.createZKClient("127.0.0.1:" + clientPorts[leader]);
        client.create("/warm", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        harness.holdAcksOf("/ahead");
        CountDownLatch aheadDone = new CountDownLatch(1);
        AtomicInteger aheadRc = new AtomicInteger(Integer.MIN_VALUE);
        client.create("/ahead", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (rc, path, ctx, name) -> {
            aheadRc.set(rc);
            aheadDone.countDown();
        }, null);
        await("the follower did not log the held create", () -> harness.ackHeldFrom(follower));
        long ahead = harness.heldZxid();

        restart(follower, () -> harness.pauseForwardingOnce(follower));
        harness.awaitForwardingPaused();
        harness.releaseAcks();
        harness.awaitCommitted();
        harness.releaseForwarding();
        assertTrue(aheadDone.await(CausynthQuorumHarness.WAIT_SECONDS, TimeUnit.SECONDS), "the held create did not finish");
        assertEquals(Code.OK.intValue(), aheadRc.get());

        assertTrue(ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[follower], CONNECTION_TIMEOUT),
            "the follower did not rejoin");
        await("the follower did not apply the held create", () -> has(follower, "/ahead"));
        client.create("/after", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        long after = peers[leader].getQuorumPeer().getZkDb().getDataTreeLastProcessedZxid();
        assertEquals(ahead + 1, after, "the follow-up create should be the next transaction");
        await("the follower did not apply the follow-up create",
            () -> has(follower, "/after"));

        restart(follower, () -> { });
        assertTrue(ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[follower], CONNECTION_TIMEOUT),
            "the follower did not restart");
        assertTrue(has(follower, "/ahead") && has(follower, "/after"), "the restarted follower lost a create");
    }

    private void restart(int sid, Runnable beforeStart) throws InterruptedException {
        peers[sid].shutdown();
        assertTrue(ClientBase.waitForServerDown("127.0.0.1:" + clientPorts[sid], CONNECTION_TIMEOUT),
            "server " + sid + " did not stop");
        beforeStart.run();
        peers[sid].start();
    }

    private boolean has(int sid, String path) {
        QuorumPeer peer = peers[sid].getQuorumPeer();
        ZKDatabase db = peer == null ? null : peer.getZkDb();
        return db != null && db.getNode(path) != null;
    }

    private int role(boolean leading) throws InterruptedException {
        AtomicInteger found = new AtomicInteger(-1);
        await(leading ? "no leader" : "no follower", () -> {
            for (int sid = 1; sid <= PARTICIPANTS; sid++) {
                QuorumPeer peer = peers[sid].getQuorumPeer();
                if (peer != null && (leading ? peer.leader != null : peer.follower != null)) {
                    found.set(sid);
                    return true;
                }
            }
            return false;
        });
        return found.get();
    }
}
