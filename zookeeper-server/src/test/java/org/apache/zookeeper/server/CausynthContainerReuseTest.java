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

package org.apache.zookeeper.server;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.CausynthZkCluster;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker's workload of zk-bugs-container-reuse: a container that lost
 * its last child is cleaned up while a client replaces it with a persistent
 * node at the same path and reads it back.
 *
 * <p>ContainerManager#checkContainers picks the empty container as a
 * candidate and posts a deleteContainer request that names only its path;
 * PrepRequestProcessor checks only that the node at the path has no children
 * and is not ephemeral.  The recording lets the cleanup run first: the
 * client's delete finds nothing and its replacement stays.  The manager is
 * the test's own, as CreateContainerTest's, and checks once.</p>
 */
public class CausynthContainerReuseTest extends ClientBase {

    private static final String PATH = "/causynth-container";
    /** When the manager checks, after it starts. */
    private static final int CHECK_MS = 100;
    /** No wait between two candidates' deletions. */
    private static final int NO_PACING = Integer.MAX_VALUE;
    /** Long enough for the check's cleanup to be done first. */
    private static final long SETTLE_MS = 2000;

    @Test
    public void keepsTheReplacementOfACleanedContainer() throws Exception {
        ZooKeeper zk = createClient();
        zk.create(PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.CONTAINER);
        zk.create(PATH + "/child", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.delete(PATH + "/child", -1);

        ZooKeeperServer server = serverFactory.getZooKeeperServer();
        ContainerManager cleanup = CausynthZkCluster.serverWork(server, () -> {
            ContainerManager manager = new ContainerManager(server.getZKDatabase(), server.firstProcessor,
                CHECK_MS, NO_PACING) {
                @Override
                public void checkContainers() throws InterruptedException {
                    super.checkContainers();
                    stop();
                }
            };
            manager.start();
            return manager;
        });
        try {
            Thread.sleep(SETTLE_MS);
            try {
                zk.delete(PATH, -1);
            } catch (KeeperException.NoNodeException cleaned) {
                // the cleanup's deletion came first
            }
            zk.create(PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.getData(PATH, false, null);
        } finally {
            cleanup.stop();
        }
    }
}
