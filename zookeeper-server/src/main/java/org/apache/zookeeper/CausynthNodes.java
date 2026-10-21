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

package org.apache.zookeeper;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.quorum.QuorumPeer;

/**
 * GraphChecker: the nodes of a JVM that is a GraphChecker workload ({@link
 * CausynthZk#AUTOMATIC} set), named alike in the recording and in every
 * replay, and the window the workload is recorded in; anywhere else it does
 * nothing and GraphChecker is never loaded.
 *
 * <p>Every client is a node, {@code auto0/client<i>} in the order the
 * clients are made ({@link #client}, from ZooKeeper's constructor), with its
 * connection and that connection's send and event threads, and asks for a
 * session that never expires ({@link #sessionTimeout}).  Every quorum peer is
 * a node by its server id, {@code auto0/peer<sid>} ({@link #starting}, from
 * {@code QuorumPeer#start}): a peer's role changes with every election and
 * may differ between two runs, its id never does.  A peer started again
 * under an id already started is the same node in its next life (its
 * epoch), and its start -- the database it loads, the threads it starts --
 * belongs to that life.  The threads a peer makes later, as it takes a role
 * in an election, are its own as they are made ({@link #made}, from
 * ZooKeeperThread's constructor): each peer thread carries its peer to the
 * threads it starts.</p>
 *
 * <p>The window opens on the test's thread with one request ({@code test}):
 * where a ClientBase test made its first client, else at the first request
 * a client queues on a thread of the test in a JVM running a quorum, once
 * the test has waited for the quorum to serve; a case that records the
 * ensemble's start opens it as the test starts the first peer, under a
 * client of its own.  It closes as the test tears down.</p>
 *
 * <p>An election ends only once every peer the test has started, and not
 * shut down since, has voted ({@link #everyLivePeerVoted}): the server with
 * the highest id among them leads, whichever peers happened to be up when a
 * quorum of them agreed.</p>
 */
public final class CausynthNodes {

    /** Longer than any case runs: a bound that cannot expire. */
    public static final int NEVER_MS = (int) TimeUnit.HOURS.toMillis(6);
    private static final boolean ON = CausynthZk.automatic();
    private static final String NODE = "CLUSTER_NODE";
    private static final String CLIENT = "EXTERNAL_APP";
    private static final String PEER = "ZK_PEER";
    private static final String PREFIX = "auto0/";
    private static final String TEST = "test";
    /** Each peer's node by server id: one anchor for all of its lives. */
    private static final Map<Long, Object> PEERS = new HashMap<>();
    private static final Map<Object, Long> EPOCHS = new IdentityHashMap<>();
    /** The server ids the test has started and not shut down. */
    private static final Set<Long> LIVE = ConcurrentHashMap.newKeySet();
    /** The peer this thread runs for, carried to every thread it starts. */
    private static final InheritableThreadLocal<QuorumPeer> RUNNING = new InheritableThreadLocal<>();
    private static final Object STARTUP_CLIENT = new Object();
    private static int clients;
    private static volatile boolean opened;
    private static boolean closed;
    private static long testRequest;

    private CausynthNodes() {
    }

    public static boolean automatic() {
        return ON;
    }

    /** The session timeout a client asks for: the caller's, or none. */
    public static int sessionTimeout(int timeout) {
        return ON ? NEVER_MS : timeout;
    }

    /** A client is made: the next client node, with its connection's threads. */
    static synchronized void client(ZooKeeper client, ClientCnxn connection) {
        if (!ON) {
            return;
        }
        MessageDoors.registerNode(client, CLIENT, "ZK_CLIENT", nextClient(), connection,
            connection.sendThread, connection.eventThread);
    }

    /**
     * A peer starts: its node, registered with its database, logs and
     * connection factories, at its next life if its id started before; the
     * start runs as that life's node startup, and the threads it starts are
     * the peer's.  Its learners wait as long as a replay is slower.
     *
     * @return what {@link #started} ends, or null
     */
    public static Object starting(QuorumPeer peer, Object... parts) {
        if (!ON) {
            return null;
        }
        long sid = peer.getMyId();
        Object node;
        synchronized (CausynthNodes.class) {
            String name = PREFIX + "peer" + sid;
            node = PEERS.get(sid);
            if (node == null) {
                node = new Object();
                PEERS.put(sid, node);
                EPOCHS.put(node, 0L);
                MessageDoors.registerNode(node, NODE, PEER, name);
            } else {
                MessageDoors.restartSource(node, NODE, PEER, name, EPOCHS.merge(node, 1L, Long::sum));
            }
            LIVE.add(sid);
            List<Object> aliases = new ArrayList<>();
            aliases.add(peer);
            for (Object part : parts) {
                if (part != null) {
                    aliases.add(part);
                }
                if (part instanceof ServerCnxnFactory) {
                    aliases.addAll(threadsOf(part));
                }
            }
            for (Object alias : aliases) {
                MessageDoors.registerSourceAlias(alias, node);
            }
        }
        int limit = Math.max(1, NEVER_MS / peer.getTickTime());
        peer.setInitLimit(limit);
        peer.setSyncLimit(limit);
        peer.setConnectToLearnerMasterLimit(limit);
        Startup startup = new Startup(RUNNING.get(), MessageDoors.beginNodeStartup(node));
        RUNNING.set(peer);
        return startup;
    }

    /** The start {@link #starting} began is over; a peer that failed to start is down. */
    public static void started(Object startup, QuorumPeer peer, boolean up) {
        if (startup == null) {
            return;
        }
        if (!up) {
            down(peer);
        }
        Startup ended = (Startup) startup;
        MessageDoors.endNodeStartup(ended.token);
        if (ended.caller == null) {
            RUNNING.remove();
        } else {
            RUNNING.set(ended.caller);
        }
    }

    /** The peer's own thread runs: what it starts is the peer's. */
    public static void running(QuorumPeer peer) {
        if (ON) {
            RUNNING.set(peer);
        }
    }

    /** A thread is made: a peer's own, if a thread of the peer makes it. */
    public static void made(Thread thread) {
        if (!ON || thread.getClass().getEnclosingClass() == ClientCnxn.class) {
            return;
        }
        QuorumPeer peer = RUNNING.get();
        if (peer != null) {
            MessageDoors.registerSourceAlias(thread, peer);
        }
    }

    /** The test starts the peer of server {@code sid}; a case recording the start opens its window. */
    public static void peerStarting(long sid) {
        if (!ON) {
            return;
        }
        LIVE.add(sid);
        if (MessageDoors.startupWindow()) {
            synchronized (CausynthNodes.class) {
                if (!opened && !closed) {
                    MessageDoors.registerNode(STARTUP_CLIENT, CLIENT, "ZK_CLIENT", nextClient());
                    openWindow(STARTUP_CLIENT);
                }
            }
        }
    }

    /** The test shuts the peer down. */
    public static void down(QuorumPeer peer) {
        if (ON) {
            LIVE.remove(peer.getMyId());
        }
    }

    /** Whether every voter whose peer is live voted. */
    public static boolean everyLivePeerVoted(Set<Long> voters, Set<Long> voted) {
        if (!ON) {
            return true;
        }
        for (Long voter : voters) {
            if (LIVE.contains(voter) && !voted.contains(voter)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A client queues a request on {@code connection}: in a JVM running a
     * quorum, the test's first one opens the window.
     */
    static void requesting(ClientCnxn connection) {
        if (opened || Thread.currentThread().getClass().getEnclosingClass() == ClientCnxn.class) {
            return;
        }
        synchronized (CausynthNodes.class) {
            if (!PEERS.isEmpty()) {
                openWindow(connection);
            }
        }
    }

    /**
     * The window opens on this thread, with the test's request on {@code
     * client}'s node; once only.
     */
    public static synchronized void openWindow(Object client) {
        if (!ON || opened || closed) {
            return;
        }
        opened = true;
        MessageDoors.startRecording();
        testRequest = MessageDoors.beginRequest(client, TEST);
    }

    /** The test is over: its request ends, and so does the window. */
    public static synchronized void endTest() {
        if (opened && !closed) {
            closed = true;
            MessageDoors.endRequest(testRequest, TEST);
            MessageDoors.stopRecording();
        }
    }

    /** The threads {@code holder} keeps, alone or in a collection. */
    public static List<Object> threadsOf(Object holder) {
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
    public static Object field(Object holder, String name) {
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

    private static synchronized String nextClient() {
        return PREFIX + "client" + clients++;
    }

    private static final class Startup {

        private final QuorumPeer caller;
        private final long token;

        private Startup(QuorumPeer caller, long token) {
            this.caller = caller;
            this.token = token;
        }
    }
}
