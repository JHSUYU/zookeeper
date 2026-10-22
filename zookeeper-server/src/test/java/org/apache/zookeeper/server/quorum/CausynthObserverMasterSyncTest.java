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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * GraphChecker workload of ZOOKEEPER-5088 (apache/zookeeper PR 2454), the passing shape of
 * ObserverMasterPendingProposalTest. Follower F restarts while the leader holds a create outstanding, so F receives
 * it as a PROPOSAL during its sync and logs it after UPTODATE, before its ObserverMaster starts; the OM never queues
 * it. The create then commits and the observer O, restarted after the create's reply, syncs through F's
 * ObserverMaster. Natively F has applied the create, so O's DIFF carries it and O follows the next create. Had O's
 * sync read F's database before that apply, O would get an empty DIFF, no INFORM of the create, and the next INFORM
 * throws at ZKDatabase.addCommittedProposal.
 */
public class CausynthObserverMasterSyncTest extends QuorumPeerTestBase {

    private static final int PARTICIPANTS = 3;
    private static final int OBSERVER = PARTICIPANTS + 1;

    private final CausynthQuorumHarness harness = new CausynthQuorumHarness();
    private final MainThread[] peers = new MainThread[OBSERVER + 1];
    private final int[] clientPorts = new int[OBSERVER + 1];
    private final int[] observerMasterPorts = new int[PARTICIPANTS + 1];

    /** Before QuorumPeerTestBase#tearDown closes the client and shuts the peers down. */
    @AfterEach
    public void releaseHolds() {
        harness.abort();
    }

    @Test
    @Timeout(value = 120)
    public void aProposalPendingAtTheObserverMastersStart() throws Exception {
        ClientBase.setupTestEnv();
        StringBuilder quorumCfg = new StringBuilder();
        for (int sid = 1; sid <= OBSERVER; sid++) {
            clientPorts[sid] = PortAssignment.unique();
            quorumCfg.append(String.format("server.%d=127.0.0.1:%d:%d:%s;127.0.0.1:%d%n", sid,
                PortAssignment.unique(), PortAssignment.unique(), sid <= PARTICIPANTS ? "participant" : "observer",
                clientPorts[sid]));
            if (sid <= PARTICIPANTS) {
                observerMasterPorts[sid] = PortAssignment.unique();
            }
        }
        for (int sid = 1; sid <= PARTICIPANTS; sid++) {
            peers[sid] = harness.participant(sid, clientPorts[sid], quorumCfg.toString(),
                config(observerMasterPorts[sid]));
        }
        servers = new Servers();
        servers.mt = new MainThread[] {peers[1], peers[2], peers[3]};
        for (int sid = 1; sid <= PARTICIPANTS; sid++) {
            peers[sid].start();
        }
        for (int sid = 1; sid <= PARTICIPANTS; sid++) {
            assertTrue(ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[sid], CONNECTION_TIMEOUT),
                "participant " + sid + " did not start");
        }
        int leader = role(true);
        int follower = role(false);

        peers[OBSERVER] = new MainThread(OBSERVER, clientPorts[OBSERVER], PortAssignment.unique(), quorumCfg.toString(),
            config(observerMasterPorts[follower]));
        servers.mt = new MainThread[] {peers[1], peers[2], peers[3], peers[OBSERVER]};
        peers[OBSERVER].start();
        assertTrue(ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[OBSERVER], CONNECTION_TIMEOUT),
            "the observer did not start");

        ZooKeeper client = ClientBase.createZKClient("127.0.0.1:" + clientPorts[leader]);
        servers.zk = new ZooKeeper[] {client};
        client.create("/warm", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        long warm = database(leader).getDataTreeLastProcessedZxid();
        await("the observer did not apply the warm-up create", () -> has(OBSERVER, "/warm"));

        stop(OBSERVER);
        stop(follower);

        harness.holdAcksOf("/pending");
        CountDownLatch pendingDone = new CountDownLatch(1);
        AtomicInteger pendingRc = new AtomicInteger(Integer.MIN_VALUE);
        client.create("/pending", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (rc, path, ctx, name) -> {
            pendingRc.set(rc);
            pendingDone.countDown();
        }, null);
        int otherFollower = PARTICIPANTS * (PARTICIPANTS + 1) / 2 - leader - follower;
        harness.awaitAckFrom(leader);
        harness.awaitAckFrom(otherFollower);
        long pending = harness.trackedZxid();
        assertEquals(warm + 1, pending, "the held create should be the next transaction");

        peers[follower].start();
        harness.awaitSync(follower, peers[follower]);
        harness.awaitAckFrom(follower);
        harness.releaseAcks();
        assertTrue(pendingDone.await(CausynthQuorumHarness.WAIT_SECONDS, TimeUnit.SECONDS),
            "the held create did not finish");
        assertEquals(Code.OK.intValue(), pendingRc.get());

        peers[OBSERVER].start();
        await("the observer did not sync through the follower's ObserverMaster", () -> {
            QuorumPeer peer = peers[OBSERVER].getQuorumPeer();
            Observer o = peer == null ? null : peer.observer;
            ZooKeeperServer server = peer == null ? null : peer.getActiveServer();
            return o != null && o.getSocket() != null && o.getSocket().getPort() == observerMasterPorts[follower]
                && server != null && server.isRunning();
        });

        client.create("/after", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        long after = database(leader).getDataTreeLastProcessedZxid();
        assertEquals(pending + 1, after, "the follow-up create should be the next transaction");
        await("the observer did not apply the held and the follow-up create",
            () -> has(OBSERVER, "/pending") && has(OBSERVER, "/after")
                && database(OBSERVER).getmaxCommittedLog() == after);
        assertTrue(peers[OBSERVER].getQuorumPeer().getActiveServer().isRunning(), "the observer stopped serving");

        // The observer's life and F's ObserverMaster handler serving it end inside the test: each occurrence returns
        // only when its peer stops, and a recorded window closes before tear-down stops it.
        List<Thread> serving = handlersOf(OBSERVER);
        peers[OBSERVER].shutdown();
        peers[OBSERVER].join(TimeUnit.SECONDS.toMillis(CausynthQuorumHarness.WAIT_SECONDS));
        for (Thread handler : serving) {
            handler.join(TimeUnit.SECONDS.toMillis(CausynthQuorumHarness.WAIT_SECONDS));
            assertFalse(handler.isAlive(), "the ObserverMaster handler of the observer did not end");
        }
    }

    /** The learner handlers serving server {@code sid} (here F's ObserverMaster's, the observer's only master). */
    private static List<Thread> handlersOf(long sid) {
        Thread[] threads = new Thread[Thread.activeCount() * 2 + 16];
        int count = Thread.enumerate(threads);
        List<Thread> handlers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            if (threads[i] instanceof LearnerHandler && ((LearnerHandler) threads[i]).getSid() == sid) {
                handlers.add(threads[i]);
            }
        }
        return handlers;
    }

    private static String config(int observerMasterPort) {
        return String.format("observerMasterPort=%d%nsnapCount=100000%nadmin.enableServer=false%n", observerMasterPort);
    }

    /** QuorumBase#shutdown joins the peer: the server is down when this returns. */
    private void stop(int sid) throws InterruptedException {
        peers[sid].shutdown();
    }

    private ZKDatabase database(int sid) {
        QuorumPeer peer = peers[sid].getQuorumPeer();
        return peer == null ? null : peer.getZkDb();
    }

    private boolean has(int sid, String path) {
        ZKDatabase db = database(sid);
        return db != null && db.getNode(path) != null;
    }

    private int role(boolean leading) throws InterruptedException {
        AtomicInteger found = new AtomicInteger(-1);
        await(leading ? "no leader" : "no follower with an ObserverMaster", () -> {
            for (int sid = 1; sid <= PARTICIPANTS; sid++) {
                QuorumPeer peer = peers[sid].getQuorumPeer();
                if (peer != null
                    && (leading ? peer.leader != null : peer.follower != null && peer.follower.om != null)) {
                    found.set(sid);
                    return true;
                }
            }
            return false;
        });
        return found.get();
    }
}
