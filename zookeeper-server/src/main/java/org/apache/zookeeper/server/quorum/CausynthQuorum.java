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

package org.apache.zookeeper.server.quorum;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.CausynthZk;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * GraphChecker: the learner stream, leader (or observer master) and
 * learner, on GraphChecker's message doors, in a JVM that is a GraphChecker
 * workload ({@link CausynthZk#automatic}); anywhere else it does nothing and
 * GraphChecker is never loaded.
 *
 * <p>Every {@link QuorumPacket} is one message ({@link #TRANSPORT}, half
 * {@code REQUEST}, its verb the packet's type), owned by the QuorumPeer of
 * the end that sends or receives it.  It leaves where its producer hands it
 * to the stream ({@link #sent}: {@code LearnerHandler.queuePacket} on the
 * proposing, committing, pinging or syncing thread, {@code
 * Learner.writePacket} on the learner's), and goes on the wire where the
 * packet's bytes are written ({@link #written}): the context travels beside
 * the bytes under the link, the direction and the packet's place in it,
 * since every peer shares the JVM.  A packet written with no send before it
 * is sent where it is written.  The receiving end takes it where it reads the
 * packet ({@link #received}) and serves it until its next read, or until the
 * thread leaves the stream ({@link #left}: the learner's role ends, the
 * handler ends): the packet's processing (the sync's switch arm, {@code
 * processPacket}, the handler's switch) comes inside it.</p>
 *
 * <p>The leader's own ACK of a proposal is a message too, from the leader to
 * itself ({@link #ackingItself}): its log's acknowledgement completes a quorum
 * as a learner's ACK does, and the order of the two is the commit's.</p>
 *
 * <p>The link is the learner end's local port: the handler's remote one.
 * Each end counts the packets it writes and reads on its socket, at every
 * place a packet's bytes are written or read (the snapshot's bytes and the
 * quorum's authentication are no packets on either side), so both ends name
 * a packet alike.  Every written packet is put on the wire, carrying
 * something or not, and every read takes its place off, so what a dead link
 * left there is overwritten before a link on the same port could read
 * it.</p>
 */
public final class CausynthQuorum {

    /** The transport of the learner stream's packets, and of its fault door. */
    public static final String TRANSPORT = "ZK_QUORUM";
    private static final String ATTEMPT = "1";
    private static final String TO_LEARNER = "L";
    private static final String TO_LEADER = "F";
    private static final Object NOTHING = new Object();
    private static final AtomicLong PACKETS = new AtomicLong();
    /** The packets on the wire, by "link:direction:place": what each carries. */
    private static final Map<String, Object> WIRE = new ConcurrentHashMap<>();
    /** The packet each reading thread serves: the message it carried. */
    private static final ThreadLocal<MessageDoors.Outbound> SERVED = new ThreadLocal<>();
    /** The txn header each reading thread last took from a packet it served. */
    private static final ThreadLocal<TxnHeader> PARSED = new ThreadLocal<>();
    /** The txn packets made here, with the header each carries: its txn's zxid is the sender's. */
    private static final Txns TXNS = new Txns();

    private CausynthQuorum() {
    }

    /** The learner's end of the stream it opened on {@code sock}, or null outside a workload. */
    static End learning(QuorumPeer peer, Socket sock) {
        return CausynthZk.automatic() ? new End(peer, sock.getLocalPort(), TO_LEADER, TO_LEARNER) : null;
    }

    /** The leader's (or observer master's) end of the stream a learner opened to it on {@code sock}. */
    static End leading(QuorumPeer peer, Socket sock) {
        return CausynthZk.automatic() ? new End(peer, sock.getPort(), TO_LEARNER, TO_LEADER) : null;
    }

    /** The producer hands {@code packet} to the stream: its message leaves, in the producer's context. */
    static void sent(End end, QuorumPacket packet) {
        if (end == null || packet == null) {
            return;
        }
        MessageDoors.Outbound outbound = end.send(packet);
        synchronized (end.held) {
            end.held.computeIfAbsent(packet, unsent -> new ArrayDeque<>()).addLast(outbound);
        }
    }

    /** The bytes of {@code packet} go on the wire, next on the end's direction. */
    static void written(End end, QuorumPacket packet) {
        if (end == null || packet == null) {
            return;
        }
        MessageDoors.Outbound outbound = null;
        synchronized (end.held) {
            ArrayDeque<MessageDoors.Outbound> queued = end.held.get(packet);
            if (queued != null) {
                outbound = queued.pollFirst();
                if (queued.isEmpty()) {
                    end.held.remove(packet);
                }
            }
        }
        if (outbound == null) {
            outbound = end.send(packet);
        }
        WIRE.put(end.link + ":" + end.sends + ":" + end.written.getAndIncrement(),
            outbound.active() ? outbound : NOTHING);
    }

    /** The end read {@code packet}, next on its direction: it serves the packet's message until its next read. */
    static void received(End end, QuorumPacket packet) {
        if (end == null) {
            return;
        }
        Object carried = WIRE.remove(end.link + ":" + end.receives + ":" + end.read.getAndIncrement());
        PARSED.remove();
        if (!(carried instanceof MessageDoors.Outbound)) {
            SERVED.remove();
            MessageDoors.inbound(null, null, TRANSPORT, "", ATTEMPT, MessageDoors.REQUEST, end.peer, false);
            return;
        }
        MessageDoors.Outbound sent = (MessageDoors.Outbound) carried;
        SERVED.set(sent);
        MessageDoors.inbound(sent.trace, sent.symbolic, TRANSPORT, sent.correlationId, sent.attemptId,
            MessageDoors.REQUEST, end.peer, false);
        MessageDoors.parsed(packet);
    }

    /**
     * The leader acknowledges its own proposal {@code zxid} (AckRequestProcessor,
     * after its log has it): an ACK from the leader to itself, as a learner's ACK
     * comes over the stream, which this thread serves until {@link #served}.
     *
     * @return the served exchange, or null outside a workload
     */
    static Object ackingItself(QuorumPeer leader, long zxid) {
        if (!CausynthZk.automatic()) {
            return null;
        }
        String correlation = "zkq-ack-" + leader.getMyId() + "-" + Long.toHexString(zxid);
        MessageDoors.Outbound sent = MessageDoors.outboundRequest(TRANSPORT, correlation, ATTEMPT, leader, null,
            Leader.getPacketType(Leader.ACK), null);
        return MessageDoors.inbound(sent.trace, sent.symbolic, TRANSPORT, correlation, ATTEMPT,
            MessageDoors.REQUEST, leader, false);
    }

    /**
     * The leader {@code end} serves is about to process an ACK: a turn at the
     * leader's ACK location, so its ACKs -- a learner's on its handler, its own
     * on its log's thread -- are processed in the recording's order, and the
     * one that completes a quorum, under which the COMMIT leaves, stays the
     * recorded one.  Writes no store row.
     *
     * @return the turn, or -1 outside a workload
     */
    static long acking(End end) {
        return end == null ? -1L : acking(end.peer);
    }

    /** As {@link #acking(End)}, for the leader's own ACK. */
    static long acking(QuorumPeer leader) {
        if (!CausynthZk.automatic() || leader == null) {
            return -1L;
        }
        return MessageDoors.storing(TRANSPORT, "acks/" + leader.getMyId());
    }

    /** The ACK {@link #acking} took the turn for is processed. */
    static void acked(long turn) {
        if (turn >= 0L) {
            MessageDoors.storeEnded(TRANSPORT, turn);
        }
    }

    /** The exchange {@link #ackingItself} began is done. */
    static void served(Object exchange) {
        if (exchange != null) {
            MessageDoors.endInbound((MessageDoors.Inbound) exchange);
        }
    }

    /** The thread that read the stream is done with it: the last packet it read is served no longer. */
    static void left() {
        if (CausynthZk.automatic()) {
            SERVED.remove();
            PARSED.remove();
            MessageDoors.endInbound();
        }
    }

    /**
     * A learner logs {@code request}'s txn while it serves a packet (the
     * PROPOSAL, or the sync's UPTODATE for what the sync held): the txn keeps
     * that packet's message, which its log's ACK leaves under ({@link
     * #servingLogged}), whatever request the batching log thread took last.
     */
    static void logging(Request request) {
        if (CausynthZk.automatic() && request != null) {
            request.causynthLogged = SERVED.get();
        }
    }

    /**
     * The leader logs its own proposal {@code request}: a PROPOSAL from the
     * leader to itself, as its learners receive one, which its log's own ACK
     * leaves under.
     */
    static void proposing(QuorumPeer leader, Request request) {
        if (!CausynthZk.automatic() || leader == null || request == null) {
            return;
        }
        String correlation = "zkq-proposal-" + leader.getMyId() + "-" + Long.toHexString(request.zxid);
        MessageDoors.Outbound sent = MessageDoors.outboundRequest(TRANSPORT, correlation, ATTEMPT, leader, null,
            Leader.getPacketType(Leader.PROPOSAL), null);
        if (sent.active()) {
            request.causynthLogged = sent;
        }
    }

    /**
     * The log acknowledges {@code request}'s txn: this thread serves the
     * message the txn was logged under until {@link #served}.
     *
     * @return the served exchange, or null
     */
    static Object servingLogged(QuorumPeer peer, Request request) {
        Object logged = request == null ? null : request.causynthLogged;
        if (!(logged instanceof MessageDoors.Outbound)) {
            return null;
        }
        MessageDoors.Outbound message = (MessageDoors.Outbound) logged;
        return MessageDoors.inbound(message.trace, message.symbolic, TRANSPORT, message.correlationId,
            message.attemptId, MessageDoors.REQUEST, peer, false);
    }

    /**
     * {@code packet} carries the txn of {@code header} (its data is the txn's
     * bytes): the packet's message is the header, so its fields cross as the
     * sender's (the door walks it), and a learner that decodes the txn from
     * the packet takes them ({@link #parsedTxn}).
     */
    static void carrying(QuorumPacket packet, TxnHeader header) {
        if (CausynthZk.automatic() && packet != null && header != null) {
            TXNS.put(packet, header);
        }
    }

    /**
     * A copy of {@code header} set field by field: a txn resent from the
     * committed log (a learner's sync) carries its fields as this send's
     * export, not as the one that first proposed them.  Null when not
     * automatic.
     */
    static TxnHeader copyOf(TxnHeader header) {
        if (!CausynthZk.automatic() || header == null) {
            return null;
        }
        TxnHeader copy = new TxnHeader();
        copy.setClientId(header.getClientId());
        copy.setCxid(header.getCxid());
        copy.setZxid(header.getZxid());
        copy.setTime(header.getTime());
        copy.setType(header.getType());
        return copy;
    }

    /**
     * The thread decoded {@code header} from the packet it serves: the
     * header's fields are the ones the packet's sender carried.  Outside a
     * served packet (a log's replay at startup), nothing.
     */
    static void parsedTxn(TxnHeader header) {
        if (!CausynthZk.automatic() || header == null || SERVED.get() == null) {
            return;
        }
        PARSED.set(header);
        MessageDoors.parsed(header);
    }

    /** The txn header this thread last took from a packet it served, or null. */
    static TxnHeader parsedHeader() {
        return CausynthZk.automatic() ? PARSED.get() : null;
    }

    /** The txn packets and their headers, by packet identity, held no longer than the packet. */
    private static final class Txns {

        private final Map<Held, TxnHeader> held = new HashMap<>();
        private final ReferenceQueue<QuorumPacket> gone = new ReferenceQueue<>();

        synchronized void put(QuorumPacket packet, TxnHeader header) {
            for (Object dropped; (dropped = gone.poll()) != null; ) {
                held.remove(dropped);
            }
            held.put(new Held(packet, gone), header);
        }

        synchronized TxnHeader get(QuorumPacket packet) {
            return held.get(new Held(packet, null));
        }
    }

    /** A packet held by identity, weakly. */
    private static final class Held extends WeakReference<QuorumPacket> {

        private final int hash;

        Held(QuorumPacket packet, ReferenceQueue<QuorumPacket> gone) {
            super(packet, gone);
            hash = System.identityHashCode(packet);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Held)) {
                return false;
            }
            QuorumPacket packet = get();
            return packet != null && packet == ((Held) other).get();
        }
    }

    /** One end of a learner stream: its peer, its link, its counts and the packets sent but not yet written. */
    static final class End {

        private final QuorumPeer peer;
        private final int link;
        private final String sends;
        private final String receives;
        private final AtomicLong written = new AtomicLong();
        private final AtomicLong read = new AtomicLong();
        private final Map<QuorumPacket, ArrayDeque<MessageDoors.Outbound>> held = new IdentityHashMap<>();

        private End(QuorumPeer peer, int link, String sends, String receives) {
            this.peer = peer;
            this.link = link;
            this.sends = sends;
            this.receives = receives;
        }

        private MessageDoors.Outbound send(QuorumPacket packet) {
            TxnHeader header = TXNS.get(packet);
            return MessageDoors.outboundRequest(TRANSPORT, "zkq-" + PACKETS.incrementAndGet(), ATTEMPT, peer,
                null, Leader.getPacketType(packet.getType()), header == null ? packet : header);
        }
    }
}
