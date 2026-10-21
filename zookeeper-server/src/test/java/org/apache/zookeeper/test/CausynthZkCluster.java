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

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.CausynthNodes;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * What makes a {@link ClientBase} test a GraphChecker workload as it is
 * written, when {@link CausynthNodes#automatic} holds on its JVM: its server
 * registered as a node, and no session it grants expiring on the wall clock.
 * Its clients are nodes of their own, made so in ZooKeeper's constructor, and
 * the first one {@link ClientBase#createClient} makes opens the recorded
 * window, with one request ({@code test}) on the thread that made it, which
 * runs the rest of the test's set-up and the test; the window closes when
 * ClientBase tears down ({@link CausynthNodes}).
 *
 * <p>Each server is registered whole as {@code auto0/server<i>}: the
 * ZooKeeperServer, its request processors and throttler, its session
 * tracker, its connection factory and that factory's threads, and its
 * database -- the objects GraphChecker's attribution finds on a thread's
 * frames.</p>
 *
 * <p>No bound expires: the recording runs on a JIT and every replay on an
 * interpreter many times slower.  A server grants any session timeout up to
 * {@link CausynthNodes#NEVER_MS}, and a client asks for that much, so no
 * session expires and no client pings or gives up on a read while a replay
 * waits for a turn.</p>
 */
public final class CausynthZkCluster {

    private static final String NODE = "CLUSTER_NODE";

    private static final Map<ZooKeeperServer, String> SERVERS = new IdentityHashMap<>();

    private CausynthZkCluster() {
    }

    public static boolean automatic() {
        return CausynthNodes.automatic();
    }

    /** Before the server starts: no session it grants expires. */
    public static void configure(ZooKeeperServer server) {
        server.setMaxSessionTimeout(CausynthNodes.NEVER_MS);
    }

    /** A server that is up, with every object it runs its work through. */
    public static synchronized void registerServer(ZooKeeperServer server) {
        if (SERVERS.containsKey(server)) {
            return;
        }
        String name = "auto0/server" + SERVERS.size();
        SERVERS.put(server, name);
        List<Object> aliases = new ArrayList<>();
        // PrepRequestProcessor -> SyncRequestProcessor -> FinalRequestProcessor
        for (Object processor = CausynthNodes.field(server, "firstProcessor"); processor != null;
             processor = CausynthNodes.field(processor, "nextProcessor")) {
            aliases.add(processor);
        }
        aliases.add(CausynthNodes.field(server, "requestThrottler"));
        aliases.add(server.getSessionTracker());
        ZKDatabase database = server.getZKDatabase();
        aliases.add(database);
        aliases.add(database.getDataTree());
        Object factory = server.getServerCnxnFactory();
        aliases.add(factory);
        aliases.addAll(CausynthNodes.threadsOf(factory));
        MessageDoors.registerNode(server, NODE, "ZK_SERVER", name, aliases.toArray());
    }

    /**
     * Work the test runs for a registered server beside the server's own
     * objects (a ContainerManager it makes): each of {@code parts}, the
     * threads it keeps and those of its timers belong to the server's node.
     */
    public static synchronized void registerServerWork(ZooKeeperServer server, Object... parts) {
        if (!SERVERS.containsKey(server)) {
            return;
        }
        for (Object part : parts) {
            MessageDoors.registerSourceAlias(part, server);
            for (Object thread : CausynthNodes.threadsOf(part)) {
                MessageDoors.registerSourceAlias(thread, server);
            }
        }
    }
}
