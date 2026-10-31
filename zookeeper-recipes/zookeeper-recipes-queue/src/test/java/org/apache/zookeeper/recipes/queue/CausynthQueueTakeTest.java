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

package org.apache.zookeeper.recipes.queue;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.Test;

/**
 * GraphChecker's workload of zk-inj-05, from DistributedQueueTest#testTakeWait1 with the order of its two
 * threads swapped: an offer thread offers at once, and a take thread waits TAKE_DELAY_MS and takes.
 * Natively the take finds the offered element and reads it (its getData is the declared target); the
 * stock testTakeWait1, whose take looks first, is the native demonstration of the case's hang.
 */
public class CausynthQueueTakeTest extends ClientBase {

    private static final String DIR = "/causynth-queue";
    private static final String ELEMENT = "Hello World";
    private static final long TAKE_DELAY_MS = 1000;

    @Test
    public void takesAnOfferedElement() throws Exception {
        ZooKeeper zk = createClient();
        DistributedQueue queue = new DistributedQueue(zk, DIR, null);

        AtomicReference<Throwable> offerFailure = new AtomicReference<>();
        Thread offer = new Thread(() -> {
            try {
                queue.offer(ELEMENT.getBytes(UTF_8));
            } catch (Throwable t) {
                offerFailure.set(t);
            }
        }, "queue-offer");
        AtomicReference<byte[]> taken = new AtomicReference<>();
        AtomicReference<Throwable> takeFailure = new AtomicReference<>();
        Thread take = new Thread(() -> {
            try {
                Thread.sleep(TAKE_DELAY_MS);
                taken.set(queue.take());
            } catch (Throwable t) {
                takeFailure.set(t);
            }
        }, "queue-take");
        offer.start();
        take.start();
        offer.join();
        take.join();

        if (offerFailure.get() != null) {
            throw new AssertionError("the offer failed", offerFailure.get());
        }
        if (takeFailure.get() != null) {
            throw new AssertionError("the take failed", takeFailure.get());
        }
        assertEquals(ELEMENT, new String(taken.get(), UTF_8));
    }
}
