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

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.DummyWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.DisconnectableZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Workload and native demonstration of zk-inj-05 (point zk-adhoc-02, SessionTrackerImpl's
 * SessionMovedException), from QuorumTest#testSessionMove on QuorumPeerMainTest#testQuorum's
 * ensemble. A client creates a node through server H, its session moves to server A (a new handle
 * with the same session id and password, the old one told not to reconnect) and writes the node,
 * then moves back to H and writes it again. Every write must succeed: the session follows its
 * client.
 *
 * movesBetweenTheFollowerAndTheLeader is the GraphChecker run's workload: testQuorum's two servers,
 * H the follower and A the leader, so the only learner that ever revalidates the session is the
 * one follower. movesBetweenTwoFollowers is the demonstration: three servers, H and A the two
 * followers, so the session is revalidated through two learners in turn.
 */
public class CausynthSessionMoveTest extends QuorumPeerTestBase {

    private final List<MainThread> servers = new ArrayList<>();

    @AfterEach
    public void tearDown() throws Exception {
        for (MainThread server : servers) {
            server.shutdown();
        }
    }

    @Test
    public void movesBetweenTheFollowerAndTheLeader() throws Exception {
        String[] hostPorts = startEnsemble(2);
        int leader = leaderIndex();
        moveAwayAndBack(hostPorts[1 - leader], hostPorts[leader]);
    }

    @Test
    public void movesBetweenTwoFollowers() throws Exception {
        String[] hostPorts = startEnsemble(3);
        int leader = leaderIndex();
        moveAwayAndBack(hostPorts[(leader + 1) % 3], hostPorts[(leader + 2) % 3]);
    }

    private void moveAwayAndBack(String home, String away) throws Exception {
        DisconnectableZooKeeper first = new DisconnectableZooKeeper(home, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE);
        first.create("/m", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        first.dontReconnect();

        DisconnectableZooKeeper moved = new DisconnectableZooKeeper(
            away, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE, first.getSessionId(), first.getSessionPasswd());
        write(moved, 1, away);
        moved.dontReconnect();

        DisconnectableZooKeeper back = new DisconnectableZooKeeper(
            home, CONNECTION_TIMEOUT, DummyWatcher.INSTANCE, first.getSessionId(), first.getSessionPasswd());
        write(back, 2, home);
        back.close();
    }

    private static void write(DisconnectableZooKeeper zk, int version, String server) throws Exception {
        try {
            zk.setData("/m", new byte[version], -1);
        } catch (KeeperException e) {
            fail("the session's write through " + server + ", where its client now is, failed: " + e.code());
        }
    }

    private String[] startEnsemble(int n) throws Exception {
        ClientBase.setupTestEnv();
        int[] clientPorts = new int[n];
        StringBuilder quorumCfgSection = new StringBuilder();
        for (int i = 0; i < n; i++) {
            clientPorts[i] = PortAssignment.unique();
            quorumCfgSection.append(String.format("server.%d=127.0.0.1:%d:%d;%d%n",
                i + 1, PortAssignment.unique(), PortAssignment.unique(), clientPorts[i]));
        }
        for (int i = 0; i < n; i++) {
            MainThread server = new MainThread(i + 1, clientPorts[i], quorumCfgSection.toString());
            servers.add(server);
            server.start();
        }
        String[] hostPorts = new String[n];
        for (int i = 0; i < n; i++) {
            hostPorts[i] = "127.0.0.1:" + clientPorts[i];
            assertTrue(ClientBase.waitForServerUp(hostPorts[i], CONNECTION_TIMEOUT),
                "waiting for server " + (i + 1) + " being up");
        }
        return hostPorts;
    }

    private int leaderIndex() {
        for (int i = 0; i < servers.size(); i++) {
            if (servers.get(i).main.quorumPeer.getPeerState() == QuorumPeer.ServerState.LEADING) {
                return i;
            }
        }
        throw new AssertionError("no server is leading");
    }
}
