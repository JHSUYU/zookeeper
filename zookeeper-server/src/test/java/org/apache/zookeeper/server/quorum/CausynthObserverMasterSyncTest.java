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
    private ZooKeeper client;

    @AfterEach
    public void tearDown() throws Exception {
        harness.abort();
        if (client != null) {
            client.close();
        }
        for (int sid = OBSERVER; sid >= 1; sid--) {
            if (peers[sid] != null) {
                peers[sid].shutdown();
            }
        }
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
            peers[sid] = harness.participant(sid, clientPorts[sid], quorumCfg.toString(), config(observerMasterPorts[sid]));
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
        peers[OBSERVER].start();
        assertTrue(ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[OBSERVER], CONNECTION_TIMEOUT),
            "the observer did not start");

        client = ClientBase.createZKClient("127.0.0.1:" + clientPorts[leader]);
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
        await("the leader did not propose the held create", () -> harness.heldAcks() >= 2);
        long pending = harness.heldZxid();
        assertEquals(warm + 1, pending, "the held create should be the next transaction");

        peers[follower].start();
        await("the restarted follower's ObserverMaster did not start with the held create pending", () -> {
            QuorumPeer peer = peers[follower].getQuorumPeer();
            Follower f = peer == null ? null : peer.follower;
            return f != null && f.om != null && f.fzk.pendingTxns.stream().anyMatch(r -> r.zxid == pending);
        });
        harness.releaseAcks();
        assertTrue(pendingDone.await(CausynthQuorumHarness.WAIT_SECONDS, TimeUnit.SECONDS), "the held create did not finish");
        assertEquals(Code.OK.intValue(), pendingRc.get());

        peers[OBSERVER].start();
        assertTrue(ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[OBSERVER], CONNECTION_TIMEOUT),
            "the observer did not restart");
        await("the observer did not sync through the follower's ObserverMaster", () -> {
            QuorumPeer peer = peers[OBSERVER].getQuorumPeer();
            Observer o = peer == null ? null : peer.observer;
            return o != null && o.getSocket() != null && o.getSocket().getPort() == observerMasterPorts[follower];
        });

        client.create("/after", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        long after = database(leader).getDataTreeLastProcessedZxid();
        assertEquals(pending + 1, after, "the follow-up create should be the next transaction");
        await("the observer did not apply the held and the follow-up create",
            () -> has(OBSERVER, "/pending") && has(OBSERVER, "/after")
                && database(OBSERVER).getmaxCommittedLog() == after);
        assertTrue(peers[OBSERVER].getQuorumPeer().getActiveServer().isRunning(), "the observer stopped serving");
    }

    private static String config(int observerMasterPort) {
        return String.format("observerMasterPort=%d%nsnapCount=100000%nadmin.enableServer=false%n", observerMasterPort);
    }

    private void stop(int sid) throws InterruptedException {
        peers[sid].shutdown();
        assertTrue(ClientBase.waitForServerDown("127.0.0.1:" + clientPorts[sid], CONNECTION_TIMEOUT),
            "server " + sid + " did not stop");
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
                if (peer != null && (leading ? peer.leader != null : peer.follower != null && peer.follower.om != null)) {
                    found.set(sid);
                    return true;
                }
            }
            return false;
        });
        return found.get();
    }
}
