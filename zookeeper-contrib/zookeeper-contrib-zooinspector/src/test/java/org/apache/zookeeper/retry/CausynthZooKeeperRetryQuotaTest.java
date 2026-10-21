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

package org.apache.zookeeper.retry;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker's workload of zk-bugs-retry-seq-quota: ZooInspector's
 * retrying client creates two sequential children under a hard count quota
 * that fits exactly them.
 *
 * <p>{@link ZooKeeperRetry#create} makes a create that lost its connection
 * again unless {@code exists} finds the path it was asked for, which for a
 * sequential create is never the name the server gave the node.  The
 * recording loses no connection.</p>
 */
public class CausynthZooKeeperRetryQuotaTest extends ClientBase {

    private static final String PATH = "/causynth-retry-quota";
    /** The path counts itself: two children fit. */
    private static final int COUNT_HARD_LIMIT = 3;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        System.setProperty(ZooKeeperServer.ENFORCE_QUOTA, "true");
        super.setUp();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        System.clearProperty(ZooKeeperServer.ENFORCE_QUOTA);
        super.tearDown();
    }

    @Test
    public void createsTwoSequentialChildrenUnderAHardQuota() throws Exception {
        ZooKeeper zk = createClient();
        zk.create(PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        StatsTrack quota = new StatsTrack();
        quota.setCountHardLimit(COUNT_HARD_LIMIT);
        SetQuotaCommand.createQuota(zk, PATH, quota);

        CountdownWatcher watcher = new CountdownWatcher();
        ZooKeeperRetry retrying = new ZooKeeperRetry(hostPort, CONNECTION_TIMEOUT, watcher);
        try {
            watcher.waitForConnected(CONNECTION_TIMEOUT);
            retrying.create(PATH + "/n-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            retrying.create(PATH + "/n-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        } finally {
            retrying.close();
        }
    }
}
