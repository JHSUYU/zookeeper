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

package org.apache.zookeeper.recipes.lock;

import java.util.concurrent.atomic.AtomicReference;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GraphChecker's workload of zk-inj-02: two lockers contend for a
 * {@link WriteLock} whose directory has a hard count quota that fits exactly
 * the directory and one lock node per locker.
 *
 * <p>A locker's {@link ProtocolSupport#retryOperation} runs its lock
 * operation again after a ConnectionLoss; the operation must then find the
 * lock node the lost reply created instead of making another.  The recording
 * loses no connection: the first locker takes the lock, the second queues
 * behind it, and the second's create is the quota's last fit.</p>
 *
 * <p>The second locker runs on a thread of its own, started after the first
 * locker's lock returns and joined: its requests are then rooted at that
 * thread's start, so how many requests the first locker made before it (one
 * more in a world where the first locker's create is made again) does not
 * change which request of the second locker its create is.  After it the test
 * makes one more request, so the server's request processor takes another
 * item after the second locker's create whatever that create's outcome.</p>
 */
public class CausynthWriteLockQuotaTest extends ClientBase {

    private static final Logger LOG = LoggerFactory.getLogger(CausynthWriteLockQuotaTest.class);
    private static final String DIR = "/causynth-locks";
    /** The directory counts itself: one lock node per locker fits. */
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
    public void twoLockersUnderAHardQuota() throws Exception {
        ZooKeeper admin = createClient();
        admin.create(DIR, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        StatsTrack quota = new StatsTrack();
        quota.setCountHardLimit(COUNT_HARD_LIMIT);
        SetQuotaCommand.createQuota(admin, DIR, quota);

        WriteLock first = new WriteLock(createClient(), DIR, null);
        LOG.info("the first locker holds the lock: {}", first.lock());
        WriteLock second = new WriteLock(createClient(), DIR, null);
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        Thread secondLocker = new Thread(() -> {
            try {
                LOG.info("the second locker holds the lock: {}", second.lock());
            } catch (Throwable t) {
                secondFailure.set(t);
            }
        }, "second-locker");
        secondLocker.start();
        secondLocker.join();
        admin.exists(DIR, false);
        if (secondFailure.get() != null) {
            throw new AssertionError("the second locker failed", secondFailure.get());
        }
    }
}
