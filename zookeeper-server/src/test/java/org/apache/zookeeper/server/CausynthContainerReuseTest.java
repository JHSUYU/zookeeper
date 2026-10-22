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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.CausynthZkCluster;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker's workload of zk-bugs-container-reuse: a container that lost
 * its last child is a cleanup candidate while a client replaces it with a
 * persistent node at the same path and reads it back.
 *
 * <p>ContainerManager#checkContainers picks the empty container as a
 * candidate and posts a deleteContainer request that names only its path;
 * PrepRequestProcessor checks only that the node at the path has no children
 * and is not an ephemeral, so it deletes whatever node holds the path when
 * the request gets there.  The manager is the test's own, as
 * CreateContainerTest's, and checks once.</p>
 *
 * <p>The manager submits a candidate's deletion through a pool of one
 * worker on a {@link SynchronousQueue} and waits for it, so its check stays
 * synchronous as upstream's and GraphChecker can hold the submission where it
 * runs.  The worker posts the deletion {@link #POST_DELAY_MS} after the scan:
 * the window between the scan and the deletion's PrepRequestProcessor, which
 * upstream leaves to the scheduler, is wide enough that the recording has the
 * client's delete, create and read inside it, and the cleanup then deletes the
 * replacement after the client's read.  The bug's world is that deletion
 * before the read.</p>
 */
public class CausynthContainerReuseTest extends ClientBase {

    private static final String PATH = "/causynth-container";
    /** When the manager checks, after it starts. */
    private static final int CHECK_MS = 100;
    /** No wait between two candidates' deletions. */
    private static final int NO_PACING = Integer.MAX_VALUE;
    /** When the client replaces the container, after the manager starts: after its scan. */
    private static final long AFTER_SCAN_MS = 1000;
    /** How long the worker waits after the scan before it posts a deletion. */
    private static final long POST_DELAY_MS = 3000;
    /** The bound on the wait for the manager's check to end. */
    private static final long CHECKED_S = 30;

    @Test
    public void keepsTheReplacementOfACleanedContainer() throws Exception {
        ZooKeeper zk = createClient();
        zk.create(PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.CONTAINER);
        zk.create(PATH + "/child", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.delete(PATH + "/child", -1);

        ZooKeeperServer server = serverFactory.getZooKeeperServer();
        OneCheck cleanup = CausynthZkCluster.serverWork(server, () -> {
            OneCheck manager = new OneCheck(server);
            manager.start();
            return manager;
        });
        try {
            Thread.sleep(AFTER_SCAN_MS);
            try {
                zk.delete(PATH, -1);
            } catch (KeeperException.NoNodeException cleaned) {
                // the cleanup's deletion came first
            }
            zk.create(PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.getData(PATH, false, null);
            cleanup.checked.await(CHECKED_S, TimeUnit.SECONDS);
        } finally {
            cleanup.stop();
        }
    }

    /** The test's manager: it checks once and submits each deletion through its pool. */
    private static final class OneCheck extends ContainerManager {

        private final ThreadPoolExecutor submitter =
            new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, new SynchronousQueue<>());
        private final CountDownLatch checked = new CountDownLatch(1);

        OneCheck(ZooKeeperServer server) {
            super(server.getZKDatabase(), server.firstProcessor, CHECK_MS, NO_PACING);
        }

        @Override
        public void checkContainers() throws InterruptedException {
            try {
                super.checkContainers();
            } finally {
                stop();
                submitter.shutdown();
                checked.countDown();
            }
        }

        @Override
        protected void postDeleteRequest(Request request) throws RequestProcessor.RequestProcessorException {
            Submission submission = new Submission(request);
            submitter.execute(submission);
            try {
                submission.done.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            if (submission.failed != null) {
                throw submission.failed;
            }
        }

        /** One deletion's submission, run by the pool's worker. */
        private final class Submission implements Runnable {

            private final Request request;
            private final CountDownLatch done = new CountDownLatch(1);
            private volatile RequestProcessor.RequestProcessorException failed;

            private Submission(Request request) {
                this.request = request;
            }

            @Override
            public void run() {
                try {
                    Thread.sleep(POST_DELAY_MS);
                    OneCheck.super.postDeleteRequest(request);
                } catch (InterruptedException stopped) {
                    Thread.currentThread().interrupt();
                } catch (RequestProcessor.RequestProcessorException refused) {
                    failed = refused;
                } finally {
                    done.countDown();
                }
            }
        }
    }
}
