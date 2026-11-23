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

import static org.apache.zookeeper.server.quorum.CausymQuorumHarness.await;
import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
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
 * Causym workload of ZOOKEEPER-5094 (apache/zookeeper PR 2459). A follower F logs a create that its leader has
 * not committed, restarts, and is told to TRUNC; the create commits while F's sync is between TRUNC and NEWLEADER, so
 * the leader sends it as PROPOSAL+COMMIT before NEWLEADER and F applies it in memory only. NEWLEADER's snapshot covers
 * it. Then, in the same life, F logs a second create its leader has not committed and loses its leader: the leader
 * tells it to TRUNC again, and F's truncateLog reloads its database from its snapshot and log. Natively everything
 * passes; a first sync that ends before NEWLEADER leaves F's database ahead of its log (no snapshot, and F re-syncs
 * with the zxid its memory holds), and the second TRUNC's reload throws at ZKDatabase.addCommittedProposal: F never
 * follows again.
 */
public class CausymTruncSyncTest extends QuorumPeerTestBase {

    private static final int PARTICIPANTS = 3;

    private final CausymQuorumHarness harness = new CausymQuorumHarness();
    private final MainThread[] peers = new MainThread[PARTICIPANTS + 1];
    private final int[] clientPorts = new int[PARTICIPANTS + 1];

    /** Before QuorumPeerTestBase#tearDown closes the client and shuts the peers down. */
    @AfterEach
    public void releaseHolds() {
        harness.abort();
    }

    @Test
    @Timeout(value = 120)
    public void aTruncSyncedFollowerIsTruncatedAgain() throws Exception {
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

        ZooKeeper client = ClientBase.createZKClient("127.0.0.1:" + clientPorts[leader]);
        servers.zk = new ZooKeeper[] {client};
        client.create("/warm", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        harness.holdAcksOf("/ahead");
        CountDownLatch aheadDone = new CountDownLatch(1);
        AtomicInteger aheadRc = new AtomicInteger(Integer.MIN_VALUE);
        client.create("/ahead", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (rc, path, ctx, name) -> {
            aheadRc.set(rc);
            aheadDone.countDown();
        }, null);
        for (int sid = 1; sid <= PARTICIPANTS; sid++) {
            harness.awaitAckFrom(sid);
        }
        long ahead = harness.trackedZxid();

        restart(follower, () -> harness.pauseForwardingOnce(follower));
        harness.awaitForwardingPaused();
        harness.releaseAcks();
        harness.awaitCommitted();
        harness.releaseForwarding();
        assertTrue(aheadDone.await(CausymQuorumHarness.WAIT_SECONDS, TimeUnit.SECONDS),
            "the held create did not finish");
        assertEquals(Code.OK.intValue(), aheadRc.get());
        harness.awaitSync(follower, peers[follower]);

        // Two follow-up creates: the reload of F's second TRUNC applies both, so the second runs the out-of-order guard.
        String[] after = {"/after", "/after2"};
        for (int i = 0; i < after.length; i++) {
            harness.noteAcksOf(after[i]);
            client.create(after[i], new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            harness.awaitAckFrom(follower);
            assertEquals(ahead + 1 + i, harness.trackedZxid(), "each follow-up create should be the next transaction");
        }

        // The second TRUNC, in F's same life (its database is the object the first sync left): F logs /ahead2, which
        // the leader has not committed, and loses its leader; its follower's shutdown fast-forwards its database over
        // its log, so it elects and registers with /ahead2's zxid and the leader tells it to TRUNC to /after2.
        harness.holdAcksOf("/ahead2");
        CountDownLatch ahead2Done = new CountDownLatch(1);
        AtomicInteger ahead2Rc = new AtomicInteger(Integer.MIN_VALUE);
        client.create("/ahead2", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (rc, path, ctx, name) -> {
            ahead2Rc.set(rc);
            ahead2Done.countDown();
        }, null);
        for (int sid = 1; sid <= PARTICIPANTS; sid++) {
            harness.awaitAckFrom(sid);
        }
        assertEquals(ahead + 1 + after.length, harness.trackedZxid(), "the second held create should be the next one");

        harness.expectSync(follower);
        harness.pauseForwardingOnce(follower);
        disconnect(follower);
        harness.awaitForwardingPaused();
        harness.releaseAcks();
        harness.awaitCommitted();
        harness.releaseForwarding();
        assertTrue(ahead2Done.await(CausymQuorumHarness.WAIT_SECONDS, TimeUnit.SECONDS),
            "the second held create did not finish");
        assertEquals(Code.OK.intValue(), ahead2Rc.get());
        harness.awaitSync(follower, peers[follower]);
        assertTrue(has(follower, "/ahead") && has(follower, "/after") && has(follower, "/after2")
            && has(follower, "/ahead2"), "the follower lost a create");

        // F's life (its peer's run, where both syncs ran) ends inside the test: a recorded window closes before
        // tear-down stops it.
        peers[follower].shutdown();
        peers[follower].join(TimeUnit.SECONDS.toMillis(CausymQuorumHarness.WAIT_SECONDS));
    }

    /** Server {@code sid} loses its leader without stopping: its follower ends and its peer elects and syncs again. */
    private void disconnect(int sid) throws IOException {
        peers[sid].getQuorumPeer().follower.getSocket().close();
    }

    /** Stop server {@code sid} (QuorumBase#shutdown joins its peer) and start its next life. */
    private void restart(int sid, Runnable beforeStart) throws InterruptedException {
        peers[sid].shutdown();
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
