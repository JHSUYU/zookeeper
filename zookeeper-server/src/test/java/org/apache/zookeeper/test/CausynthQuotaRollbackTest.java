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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.ListQuotaCommand;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker's workload of zk-inj-03: a client sets a hard count quota,
 * tolerating a call that lost its connection (it may have been made), and
 * lists the quota back.
 *
 * <p>{@link SetQuotaCommand#createQuota} makes the quota in separate
 * requests -- the limit node, then the stat node -- and
 * {@link ListQuotaCommand#listQuota} reads the limit, then the stat.  The
 * quota's directory under /zookeeper/quota is made before, so the quota's
 * creates are its limit and its stat alone: a call that lost its connection
 * then made the limit whatever else it did.  The recording loses no
 * connection.</p>
 *
 * <p>The listing runs on a thread of its own, started after createQuota
 * returns and joined: its requests are then rooted at that thread's start, so
 * how many requests createQuota made before it (another number where a create
 * of it lost its reply) does not change which request of the listing reads
 * the limit.</p>
 */
public class CausynthQuotaRollbackTest extends ClientBase {

    private static final String PATH = "/causynth-quota";

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
    public void listsAQuotaSetOverALostConnection() throws Exception {
        ZooKeeper zk = createClient();
        zk.create(PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(Quotas.quotaPath(PATH), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        StatsTrack quota = new StatsTrack();
        quota.setCountHardLimit(3);
        try {
            SetQuotaCommand.createQuota(zk, PATH, quota);
        } catch (KeeperException.ConnectionLossException mayHaveBeenMade) {
            // the quota may have been made: listing it says
        }
        AtomicReference<Throwable> listFailure = new AtomicReference<>();
        Thread list = new Thread(() -> {
            try {
                ListQuotaCommand.listQuota(zk, PATH);
            } catch (Throwable t) {
                listFailure.set(t);
            }
        }, "quota-list");
        list.start();
        list.join();
        if (listFailure.get() != null) {
            throw new AssertionError("the listing failed", listFailure.get());
        }
    }
}
