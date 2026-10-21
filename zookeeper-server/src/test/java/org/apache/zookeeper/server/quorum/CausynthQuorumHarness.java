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

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import javax.security.sasl.SaslException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerTestBase.MainThread;
import org.apache.zookeeper.server.quorum.QuorumPeerTestBase.TestQPMain;
import org.apache.zookeeper.txn.CreateTxn;

/**
 * The holds of the GraphChecker quorum workloads (CausynthTruncSyncTest, CausynthObserverMasterSyncTest), in the shape
 * of ObserverMasterPendingProposalTest (apache/zookeeper PR 2454): a participant's leader is a {@link HeldLeader} that
 * can hold the ACKs of one create and pause one follower's startForwarding. A hold only makes an order controllable:
 * every proposal, commit, queue and zxid is the normal protocol's, and every override waits before it calls the
 * synchronized super, so no lock is held while waiting. Holds and waits are bounded by {@link #WAIT_SECONDS}, far
 * above a native run's seconds: a GraphChecker replay runs many times slower than its recording, and a native hang
 * is still bounded by the test's own timeout.
 */
public class CausynthQuorumHarness {

    static final long WAIT_SECONDS = 300;

    private volatile String heldPath;
    private volatile long heldZxid = -1;
    private final Set<Long> heldAckSids = ConcurrentHashMap.newKeySet();
    private volatile CountDownLatch acksReleased = new CountDownLatch(0);
    private volatile CountDownLatch committed = new CountDownLatch(1);

    private volatile long pausedSid = -1;
    private final AtomicBoolean pauseTaken = new AtomicBoolean(true);
    private volatile CountDownLatch forwardingPaused = new CountDownLatch(1);
    private volatile CountDownLatch forwardingReleased = new CountDownLatch(0);

    /** A participant whose leader is a {@link HeldLeader} of this harness. */
    MainThread participant(int sid, int clientPort, String quorumCfg, String otherCfg) throws IOException {
        return new HeldPeer(sid, clientPort, quorumCfg, otherCfg);
    }

    /** Hold every ACK of the next create of {@code path} until {@link #releaseAcks()}. */
    void holdAcksOf(String path) {
        heldZxid = -1;
        heldAckSids.clear();
        committed = new CountDownLatch(1);
        acksReleased = new CountDownLatch(1);
        heldPath = path;
    }

    /** Pause the next startForwarding for follower {@code sid} until {@link #releaseForwarding()}. */
    void pauseForwardingOnce(long sid) {
        forwardingPaused = new CountDownLatch(1);
        forwardingReleased = new CountDownLatch(1);
        pausedSid = sid;
        pauseTaken.set(false);
    }

    long heldZxid() {
        return heldZxid;
    }

    boolean ackHeldFrom(long sid) {
        return heldAckSids.contains(sid);
    }

    int heldAcks() {
        return heldAckSids.size();
    }

    void releaseAcks() {
        acksReleased.countDown();
    }

    void releaseForwarding() {
        forwardingReleased.countDown();
    }

    void awaitForwardingPaused() throws InterruptedException {
        assertTrue(forwardingPaused.await(WAIT_SECONDS, TimeUnit.SECONDS), "startForwarding was not reached");
    }

    void awaitCommitted() throws InterruptedException {
        assertTrue(committed.await(WAIT_SECONDS, TimeUnit.SECONDS), "the held create did not commit");
    }

    void abort() {
        releaseForwarding();
        releaseAcks();
    }

    static void await(String message, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), message);
    }

    private static void hold(CountDownLatch latch, String what) {
        try {
            if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the test did not release " + what);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while holding " + what, e);
        }
    }

    private final class HeldPeer extends MainThread {

        HeldPeer(int sid, int clientPort, String quorumCfg, String otherCfg) throws IOException {
            super(sid, clientPort, PortAssignment.unique(), quorumCfg, otherCfg);
        }

        @Override
        public TestQPMain getTestQPMain() {
            return new HeldQPMain();
        }
    }

    private final class HeldQPMain extends TestQPMain {

        @Override
        protected QuorumPeer getQuorumPeer() throws SaslException {
            return new HeldQuorumPeer();
        }
    }

    private final class HeldQuorumPeer extends QuorumPeer {

        HeldQuorumPeer() throws SaslException {
        }

        @Override
        protected Leader makeLeader(FileTxnSnapLog logFactory) throws IOException, X509Exception {
            return new HeldLeader(this, new LeaderZooKeeperServer(logFactory, this, getZkDb()));
        }
    }

    private final class HeldLeader extends Leader {

        HeldLeader(QuorumPeer self, LeaderZooKeeperServer zk) throws IOException, X509Exception {
            super(self, zk);
        }

        @Override
        public Proposal propose(Request request) throws XidRolloverException {
            String path = heldPath;
            if (path != null && request.getTxn() instanceof CreateTxn
                    && path.equals(((CreateTxn) request.getTxn()).getPath())) {
                heldPath = null;
                heldZxid = request.zxid;
            }
            return super.propose(request);
        }

        @Override
        public void processAck(long sid, long zxid, SocketAddress followerAddr) {
            if (zxid == heldZxid) {
                heldAckSids.add(sid);
                hold(acksReleased, "the held ACKs");
            }
            super.processAck(sid, zxid, followerAddr);
        }

        @Override
        public synchronized boolean tryToCommit(Proposal p, long zxid, SocketAddress followerAddr) {
            boolean done = super.tryToCommit(p, zxid, followerAddr);
            if (done && zxid == heldZxid) {
                committed.countDown();
            }
            return done;
        }

        @Override
        public long startForwarding(LearnerHandler handler, long lastSeenZxid) {
            if (handler.getSid() == pausedSid && pauseTaken.compareAndSet(false, true)) {
                forwardingPaused.countDown();
                hold(forwardingReleased, "startForwarding");
            }
            return super.startForwarding(handler, lastSeenZxid);
        }
    }
}
