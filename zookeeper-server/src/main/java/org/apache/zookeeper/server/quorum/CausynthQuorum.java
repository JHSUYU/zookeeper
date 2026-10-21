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
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.CausynthZk;

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
        if (!(carried instanceof MessageDoors.Outbound)) {
            MessageDoors.inbound(null, null, TRANSPORT, "", ATTEMPT, MessageDoors.REQUEST, end.peer, false);
            return;
        }
        MessageDoors.Outbound sent = (MessageDoors.Outbound) carried;
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

    /** The exchange {@link #ackingItself} began is done. */
    static void served(Object exchange) {
        if (exchange != null) {
            MessageDoors.endInbound((MessageDoors.Inbound) exchange);
        }
    }

    /** The thread that read the stream is done with it: the last packet it read is served no longer. */
    static void left() {
        if (CausynthZk.automatic()) {
            MessageDoors.endInbound();
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
            return MessageDoors.outboundRequest(TRANSPORT, "zkq-" + PACKETS.incrementAndGet(), ATTEMPT, peer,
                null, Leader.getPacketType(packet.getType()), packet);
        }
    }
}
