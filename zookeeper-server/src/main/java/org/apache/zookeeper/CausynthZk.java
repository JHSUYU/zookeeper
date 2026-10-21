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
import org.apache.jute.Record;
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
 * <p>The reply is a message too, server to client (half {@code RESPONSE}),
 * the way HBase's CausynthRpc answers: FinalRequestProcessor sends it under
 * the request's own message, the exchange its stage serves ({@link
 * #respond}), with its reply record, and what it carries waits beside the
 * bytes under the request's correlation; the client's send thread receives
 * it where readResponse has the reply for the request's packet ({@link
 * #responded}), binds the record it read ({@link #parsed}) and serves it
 * until the packet is finished ({@link #handled}): the synchronous caller's
 * wake-up and an asynchronous callback's queueing on the event thread come
 * inside it.  A packet a lost connection finished took no reply.  Not wired:
 * watch notifications (a watcher runs on the client's event thread) and a
 * quorum's forwarding between servers.</p>
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
    /** The replies on the wire, by their request's correlation: what each carries. */
    private static final Map<String, MessageDoors.Outbound> REPLIES = new ConcurrentHashMap<>();

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
        CausynthNodes.requesting(cnxn);
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
     * The reply to the request {@code exchange} serves ({@link #serve}'s)
     * leaves with {@code reply}, under the request's own message: its
     * producer is where the request was received.  Outside a served request,
     * nothing.
     */
    public static void respond(Object exchange, Object owner, Record reply) {
        MessageDoors.Inbound serving = (MessageDoors.Inbound) exchange;
        if (serving == null || serving.correlationId.isEmpty()) {
            return;
        }
        MessageDoors.Outbound outbound = MessageDoors.outbound(TRANSPORT, serving.correlationId,
            serving.attemptId, MessageDoors.RESPONSE, owner, null, reply);
        if (outbound.active()) {
            REPLIES.put(serving.correlationId, outbound);
        }
    }

    /**
     * The server's reply to the request {@code sent} sent: the send thread
     * that read it serves it until {@link #handled}.
     *
     * @return the installed exchange, or null
     */
    static Object responded(Sent sent) {
        if (sent == null) {
            return null;
        }
        MessageDoors.Outbound reply = REPLIES.remove(sent.outbound.correlationId);
        if (reply == null) {
            return null;
        }
        return MessageDoors.inbound(reply.trace, reply.symbolic, TRANSPORT, reply.correlationId,
            reply.attemptId, MessageDoors.RESPONSE, sent.cnxn, false);
    }

    /** The reply {@code exchange} serves was read into {@code reply}. */
    static void parsed(Object exchange, Record reply) {
        if (exchange != null && reply != null && MessageDoors.current() == exchange) {
            MessageDoors.parsed(reply);
        }
    }

    /** The packet {@link #responded} took the reply for is finished. */
    static void handled(Object exchange) {
        served(exchange);
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
