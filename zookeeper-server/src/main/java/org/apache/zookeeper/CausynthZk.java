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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.server.Request;

/**
 * GraphChecker: a client's request to its server on GraphChecker's message
 * doors, in a JVM that is a GraphChecker workload ({@link #AUTOMATIC} set);
 * anywhere else it does nothing and GraphChecker is never loaded.
 *
 * <p>One message per request, client to server ({@link #TRANSPORT}, half
 * {@code REQUEST}).  The client sends it where the caller queues the request
 * ({@link #request}, on the caller's thread: its context is the sender's),
 * and its send thread puts it on the wire once it numbers the request
 * ({@code Sent#wire}): the context travels alongside the bytes, under the
 * session and xid the bytes carry, since client and server share the JVM.
 * The server serves it in each stage that does the request's work ({@link
 * #serve}): PrepRequestProcessor, which validates it and makes its txn, and
 * FinalRequestProcessor, which applies it and answers.  Each stage installs
 * the request's own message, because SyncRequestProcessor batches the
 * requests it hands on (its {@code toFlush}), so the scope its queue carried
 * is not the request's.</p>
 *
 * <p>The reply is a message too, server to client (half {@code RESPONSE}):
 * FinalRequestProcessor sends it under the request's own message, where the
 * request was received ({@link #respond}), and the caller takes it into the
 * context it called from when its packet finishes ({@link #answered}: the
 * synchronous caller's thread, an asynchronous callback's event thread), so
 * what the server did before its reply happens before what the caller does
 * after it.  Not wired: watch notifications (a watcher runs on the client's
 * event thread) and a quorum's forwarding between servers.</p>
 */
public final class CausynthZk {

    /** Set on a JVM that GraphChecker records or replays. */
    public static final String AUTOMATIC = "causynth.cluster.automatic";
    /** The transport of a client's requests, and of their fault doors. */
    public static final String TRANSPORT = "ZOOKEEPER";
    private static final boolean ON = Boolean.getBoolean(AUTOMATIC);
    private static final String ATTEMPT = "1";
    private static final AtomicLong REQUESTS = new AtomicLong();
    /** The requests on the wire, by "session:xid": what each carries. */
    private static final Map<String, MessageDoors.Outbound> WIRE = new ConcurrentHashMap<>();

    private CausynthZk() {
    }

    /** Whether this JVM is a GraphChecker workload. */
    public static boolean automatic() {
        return ON;
    }

    /**
     * A request the caller queues on {@code cnxn}: its message leaves, in the
     * caller's context.  Null for a packet of the connection's own (a ping,
     * an auth) and outside a workload.
     */
    static Sent request(ClientCnxn cnxn, RequestHeader header) {
        if (!ON || header == null || header.getType() == ZooDefs.OpCode.ping
            || header.getType() == ZooDefs.OpCode.auth) {
            return null;
        }
        MessageDoors.Outbound outbound = MessageDoors.outbound(TRANSPORT,
            "zk-" + REQUESTS.incrementAndGet(), ATTEMPT, MessageDoors.REQUEST, cnxn, null);
        return outbound.active() ? new Sent(cnxn, outbound) : null;
    }

    /**
     * The request's message, served by one stage of {@code owner}'s pipeline
     * until {@link #served}; the {@code last} stage takes it off the wire.
     *
     * @return the installed exchange, or null
     */
    public static Object serve(Request request, Object owner, boolean last) {
        if (!ON) {
            return null;
        }
        String key = key(request.sessionId, request.cxid);
        MessageDoors.Outbound sent = last ? WIRE.remove(key) : WIRE.get(key);
        if (sent == null) {
            return null;
        }
        return MessageDoors.inbound(sent.trace, sent.symbolic, TRANSPORT, sent.correlationId,
            sent.attemptId, MessageDoors.REQUEST, owner, false);
    }

    /**
     * The reply to the request this thread serves leaves, under the
     * request's own message: its producer is where the request was received.
     * Outside a served request, nothing.
     */
    public static void respond(Object owner) {
        if (!ON) {
            return;
        }
        MessageDoors.Inbound serving = MessageDoors.current();
        if (serving == null || serving.correlationId.isEmpty()) {
            return;
        }
        MessageDoors.outbound(TRANSPORT, serving.correlationId, serving.attemptId,
            MessageDoors.RESPONSE, owner, null);
    }

    /**
     * The caller takes the reply to its request into the context it called
     * from: no new occurrence, a RECV naming the reply, and whatever exchange
     * the thread was serving stays served.
     */
    static void answered(Sent sent) {
        if (sent == null) {
            return;
        }
        MessageDoors.Inbound serving = MessageDoors.setAside();
        try {
            MessageDoors.endInbound(MessageDoors.received("", TRANSPORT,
                sent.outbound.correlationId, sent.outbound.attemptId,
                MessageDoors.RESPONSE, sent.cnxn));
        } finally {
            MessageDoors.restore(serving);
        }
    }

    /** The stage {@link #serve} began is done with the request. */
    public static void served(Object exchange) {
        if (exchange != null) {
            MessageDoors.endInbound((MessageDoors.Inbound) exchange);
        }
    }

    private static String key(long session, int xid) {
        return Long.toHexString(session) + ":" + xid;
    }

    /** A queued request's message, until its send thread numbers it. */
    static final class Sent {

        private final ClientCnxn cnxn;
        private final MessageDoors.Outbound outbound;

        private Sent(ClientCnxn cnxn, MessageDoors.Outbound outbound) {
            this.cnxn = cnxn;
            this.outbound = outbound;
        }

        /** The request goes on the wire as {@code xid} of the session. */
        void wire(int xid) {
            WIRE.put(key(cnxn.getSessionId(), xid), outbound);
        }
    }
}
