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

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker's workload of zk-bugs-quota-init: a client sets a hard count
 * quota the way a ZooKeeper client makes any call, retrying one that lost its
 * connection, raises the limit with a second call on the same path, and the
 * server enforces the raised limit.
 *
 * <p>{@link SetQuotaCommand#createQuota} makes the quota in separate
 * requests -- the limit node, then the stat node -- and a call that finds the
 * limit takes the update branch, which reads the stat.  The recording loses
 * no connection, so the update is the second call's.</p>
 */
public class CausynthQuotaRetryTest extends ClientBase {

    private static final String PATH = "/causynth-quota";
    /** How often a call that lost its connection is made again. */
    private static final int ATTEMPTS = 3;

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
    public void raisesARetriedHardQuota() throws Exception {
        ZooKeeper zk = createClient();
        zk.create(PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        setCountHardLimit(zk, 2);
        setCountHardLimit(zk, 3);

        // The path counts itself: two children fit under the raised limit.
        zk.create(PATH + "/c1", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(PATH + "/c2", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertThrows(KeeperException.QuotaExceededException.class,
            () -> zk.create(PATH + "/c3", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
    }

    /** One quota call, made again when it lost its connection. */
    private static void setCountHardLimit(ZooKeeper zk, int limit) throws Exception {
        StatsTrack quota = new StatsTrack();
        quota.setCountHardLimit(limit);
        for (int attempt = 1; ; attempt++) {
            try {
                SetQuotaCommand.createQuota(zk, PATH, quota);
                return;
            } catch (KeeperException.ConnectionLossException lost) {
                if (attempt == ATTEMPTS) {
                    throw lost;
                }
            }
        }
    }
}
