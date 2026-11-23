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

import edu.uva.liftlab.causym.runtime.MessageDoors;
import edu.uva.liftlab.causym.runtime.Sessions;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CausymNodes;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * What makes a {@link ClientBase} test a Causym workload as it is
 * written, when {@link CausymNodes#automatic} holds on its JVM: its server
 * registered as a node, and no session it grants expiring on the wall clock.
 * Its clients are nodes of their own, made so in ZooKeeper's constructor, and
 * the first one {@link ClientBase#createClient} makes opens the recorded
 * window, with one request ({@code test}) on the thread that made it, which
 * runs the rest of the test's set-up and the test; the window closes when
 * ClientBase tears down ({@link CausymNodes}).
 *
 * <p>Each server is registered whole as {@code auto0/server<i>}: the
 * ZooKeeperServer, its request processors and throttler, its session
 * tracker, its connection factory and that factory's threads, and its
 * database -- the objects Causym's attribution finds on a thread's
 * frames.</p>
 *
 * <p>No bound expires: the recording runs on a JIT and every replay on an
 * interpreter many times slower.  A server grants any session timeout up to
 * {@link CausymNodes#NEVER_MS}, and a client asks for that much, so no
 * session expires and no client pings or gives up on a read while a replay
 * waits for a turn.</p>
 *
 * <p>A planted SessionExpired expires the session at the server that holds
 * it ({@link #expire}): a registered server, or a quorum's role server
 * ({@link CausymNodes#roleServers}).</p>
 */
public final class CausymZkCluster {

    private static final String NODE = "CLUSTER_NODE";

    private static final Map<ZooKeeperServer, String> SERVERS = new IdentityHashMap<>();

    static {
        if (automatic()) {
            Sessions.expirer(CausymZkCluster::expire);
        }
    }

    private CausymZkCluster() {
    }

    public static boolean automatic() {
        return CausymNodes.automatic();
    }

    /** Before the server starts: no session it grants expires. */
    public static void configure(ZooKeeperServer server) {
        server.setMaxSessionTimeout(CausymNodes.NEVER_MS);
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
        for (Object processor = CausymNodes.field(server, "firstProcessor"); processor != null;
             processor = CausymNodes.field(processor, "nextProcessor")) {
            aliases.add(processor);
        }
        aliases.add(CausymNodes.field(server, "requestThrottler"));
        aliases.add(server.getSessionTracker());
        ZKDatabase database = server.getZKDatabase();
        aliases.add(database);
        aliases.add(database.getDataTree());
        Object factory = server.getServerCnxnFactory();
        aliases.add(factory);
        aliases.addAll(CausymNodes.threadsOf(factory));
        MessageDoors.registerNode(server, NODE, "ZK_SERVER", name, aliases.toArray());
    }

    /**
     * Work the test runs for a registered server beside the server's own
     * objects (a ContainerManager it makes and starts), built in the server
     * node's startup, so every thread it creates and every task it files are
     * the server's (as HBase's JVMClusterUtil#start starts a node's thread).
     */
    public static synchronized <T> T serverWork(ZooKeeperServer server, Supplier<T> build) {
        if (!automatic() || !SERVERS.containsKey(server)) {
            return build.get();
        }
        long startup = MessageDoors.beginNodeStartup(server);
        try {
            return build.get();
        } finally {
            MessageDoors.endNodeStartup(startup);
        }
    }

    /**
     * A planted SessionExpired made real ({@code Sessions#expires}), as HBase's
     * MiniZooKeeperCluster#causymExpire: the session of the door's client
     * expired at the server that holds it, which deletes its ephemeral nodes;
     * the client's next operations fail by themselves.  The server's close is
     * awaited (bounded), so every run sees the same store after it.  False for
     * an owner that is no ZooKeeper client, or a session no server holds.
     */
    static boolean expire(Object owner) {
        if (!(owner instanceof ZooKeeper)) {
            return false;
        }
        long session = ((ZooKeeper) owner).getSessionId();
        List<ZooKeeperServer> servers;
        synchronized (CausymZkCluster.class) {
            servers = new ArrayList<>(SERVERS.keySet());
        }
        servers.addAll(CausymNodes.roleServers());
        for (ZooKeeperServer server : servers) {
            Map<Long, Integer> sessions;
            try {
                sessions = server.getZKDatabase().getSessionWithTimeOuts();
            } catch (RuntimeException shutDown) {
                continue;
            }
            if (!sessions.containsKey(session)) {
                continue;
            }
            server.expire(session);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while ((sessions.containsKey(session) || !server.getZKDatabase().getEphemerals(session).isEmpty())
                && System.nanoTime() < deadline) {
                Thread.yield();
            }
            return true;
        }
        return false;
    }
}
