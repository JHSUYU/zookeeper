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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CausynthZk;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * What makes a {@link ClientBase} test a GraphChecker workload as it is
 * written, when {@link CausynthZk#AUTOMATIC} is set on its JVM: its server and
 * its clients registered as nodes, the recorded window opened with the first
 * client and closed with the test, and nothing the test sets up expiring on
 * the wall clock.
 *
 * <p>Each server is registered whole as {@code auto0/server<i>}: the
 * ZooKeeperServer, its request processors and throttler, its session
 * tracker, its connection factory and that factory's threads, and its
 * database -- the objects GraphChecker's attribution finds on a thread's
 * frames.  Each client {@link ClientBase#createClient} makes is registered as
 * {@code auto0/client<i>} with its connection and the connection's send and
 * event threads; the first one opens the window, with one request ({@code
 * test}) on the thread that made it, which runs the rest of the test's set-up
 * and the test.  The window closes when ClientBase tears down.</p>
 *
 * <p>No bound expires: the recording runs on a JIT and every replay on an
 * interpreter many times slower.  A server grants any session timeout up to
 * {@link #NEVER_MS}, and a client asks for that much, so no session expires
 * and no client pings or gives up on a read while a replay waits for a
 * turn.</p>
 *
 * <p>Not wired: a QuorumBase ensemble's peers, and a client made other than
 * by {@link ClientBase#createClient}.</p>
 */
public final class CausynthZkCluster {

    private static final String NODE = "CLUSTER_NODE";
    private static final String CLIENT = "EXTERNAL_APP";
    /** Longer than any case runs: a bound that cannot expire. */
    public static final int NEVER_MS = (int) TimeUnit.HOURS.toMillis(6);

    private static final Map<ZooKeeperServer, String> SERVERS = new IdentityHashMap<>();
    private static final Map<ZooKeeper, String> CLIENTS = new IdentityHashMap<>();
    private static long testRequest;

    private CausynthZkCluster() {
    }

    public static boolean automatic() {
        return CausynthZk.automatic();
    }

    /** Before the server starts: no session it grants expires. */
    public static void configure(ZooKeeperServer server) {
        server.setMaxSessionTimeout(NEVER_MS);
    }

    /** The session timeout a client asks for: the test's, or none. */
    public static int sessionTimeout(int timeout) {
        return automatic() ? NEVER_MS : timeout;
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
        for (Object processor = field(server, "firstProcessor"); processor != null;
             processor = field(processor, "nextProcessor")) {
            aliases.add(processor);
        }
        aliases.add(field(server, "requestThrottler"));
        aliases.add(server.getSessionTracker());
        ZKDatabase database = server.getZKDatabase();
        aliases.add(database);
        aliases.add(database.getDataTree());
        Object factory = server.getServerCnxnFactory();
        aliases.add(factory);
        aliases.addAll(threadsOf(factory));
        MessageDoors.registerNode(server, NODE, "ZK_SERVER", name, aliases.toArray());
    }

    /**
     * A client the test made and connected; the first one opens the window
     * on this thread.
     */
    public static synchronized void registerClient(ZooKeeper client) {
        if (CLIENTS.containsKey(client)) {
            return;
        }
        String name = "auto0/client" + CLIENTS.size();
        CLIENTS.put(client, name);
        Object connection = field(client, "cnxn");
        MessageDoors.registerNode(client, CLIENT, "ZK_CLIENT", name, connection,
            field(connection, "sendThread"), field(connection, "eventThread"));
        if (testRequest == 0L) {
            MessageDoors.startRecording();
            testRequest = MessageDoors.beginRequest(client, "test");
        }
    }

    /** The test is over: its request ends, and so does the window. */
    public static synchronized void endTest() {
        if (testRequest != 0L) {
            MessageDoors.endRequest(testRequest, "test");
            testRequest = 0L;
            MessageDoors.stopRecording();
        }
    }

    /** The threads {@code holder} keeps, alone or in a collection. */
    private static List<Object> threadsOf(Object holder) {
        List<Object> threads = new ArrayList<>();
        for (Class<?> type = holder.getClass(); type != Object.class; type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                Object value = read(field, holder);
                if (value instanceof Thread) {
                    threads.add(value);
                } else if (value instanceof Collection) {
                    for (Object element : (Collection<?>) value) {
                        if (element instanceof Thread) {
                            threads.add(element);
                        }
                    }
                }
            }
        }
        return threads;
    }

    /** The value of the field {@code name} of {@code holder}'s class or a superclass, or null. */
    private static Object field(Object holder, String name) {
        for (Class<?> type = holder.getClass(); type != null; type = type.getSuperclass()) {
            try {
                return read(type.getDeclaredField(name), holder);
            } catch (NoSuchFieldException absent) {
                // the superclass's
            }
        }
        return null;
    }

    private static Object read(Field field, Object holder) {
        if (Modifier.isStatic(field.getModifiers())) {
            return null;
        }
        try {
            field.setAccessible(true);
            return field.get(holder);
        } catch (ReflectiveOperationException | RuntimeException unreadable) {
            return null;
        }
    }
}
