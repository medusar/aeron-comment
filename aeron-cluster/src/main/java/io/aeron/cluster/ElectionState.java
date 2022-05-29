/*
 * Copyright 2014-2022 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.cluster.client.ClusterException;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Election states for a {@link ConsensusModule} which get represented by a {@link #code()} stored in a
 * {@link io.aeron.Counter} of the type {@link ConsensusModule.Configuration#ELECTION_STATE_TYPE_ID}.
 */
public enum ElectionState
{
    /**
     * Consolidate local state and prepare for new leadership.
     */
    INIT(0),

    /**
     * The node is checking connectivity between nodes, which is used to ensure a successful election is possible.
     * This also prevents leadership bouncing between nodes with partial connectivity - see Coracle: Evaluating Consensus at the Internet Edge
     * Canvass members for current state and to assess if a successful leadership attempt can be mounted.
     */
    CANVASS(1),

    /**
     * Nominate member for new leadership by requesting votes.
     */
    NOMINATE(2),

    /**
     * It is a candidate, and it is waiting for vote result.
     * Await ballot outcome from members on candidacy for leadership.
     */
    CANDIDATE_BALLOT(3),

    /**
     * It is a follower, and it has voted for a candidate, and waiting for result.
     * Await ballot outcome after voting for a candidate.
     */
    FOLLOWER_BALLOT(4),

    /**
     * [Leader only] Leader is waiting for followers to replicate any missing log entries
     * Wait for followers to replicate any missing log entries to track commit position.
     */
    LEADER_LOG_REPLICATION(5),

    /**
     * Replay local log in preparation for new leadership term.
     *  [Leader only] Leader is replaying the local log
     */
    LEADER_REPLAY(6),

    /**
     * Initialise state for new leadership term.
     *  [Leader only] Leader is initialising internal state for new leadership term
     */
    LEADER_INIT(7),

    /**
     * Publish new leadership term and commit position, while awaiting followers ready.
     * [Leader only] Leader is ready, and is awaiting followers to mark themselves FOLLOWER_READY.
     */
    LEADER_READY(8),

    /**
     * Replicate missing log entries from the leader.
     *  [Follower only] Follower is replicating missing log entries from the leader
     */
    FOLLOWER_LOG_REPLICATION(9),

    /**
     * Replay local log in preparation for following new leader.
     *  [Follower only] Follower is replaying the local log in preparing for following a new leader
     */
    FOLLOWER_REPLAY(10),

    /**
     * Initialise catch-up in preparation of receiving a replay from the leader to catch up in current term.
     *  [Follower only] The node is preparing for receiving a replay from the leader to catch up in current term.
     */
    FOLLOWER_CATCHUP_INIT(11),

    /**
     * Await joining a replay from leader to catch-up.
     *  [Follower only] The node is awaiting a join from the leader for catch up.
     */
    FOLLOWER_CATCHUP_AWAIT(12),

    /**
     * Catch-up to leader until live log can be added and merged.
     * [Follower only] Follower is replaying the log in order to catch up to leader's live position and be able to join the live log
     */
    FOLLOWER_CATCHUP(13),

    /**
     * Initialise follower in preparation for joining the live log.
     *  [Follower only] Follower is initializing internal state to join the live log on the leader
     */
    FOLLOWER_LOG_INIT(14),

    /**
     * Await joining the live log from the leader.
     *  [Follower only] Follower's clustered service is processing the log,
     *  and the consensus module is awaiting this in order to join the live log of the leader
     */
    FOLLOWER_LOG_AWAIT(15),

    /**
     * Publish append position to leader to signify ready for new term.
     *  [Follower only] Follower is ready,
     *  and published the append position to the leader to let the leader know the follower is ready for the new term
     */
    FOLLOWER_READY(16),

    /**
     * Election is closed after new leader is established.
     * The election is closed
     */
    CLOSED(17);

    static final ElectionState[] STATES = values();

    private final int code;

    ElectionState(final int code)
    {
        if (code != ordinal())
        {
            throw new IllegalArgumentException(name() + " - code must equal ordinal value: code=" + code);
        }

        this.code = code;
    }

    /**
     * Code stored in a {@link io.aeron.Counter} to represent the election state.
     *
     * @return code stored in a {@link io.aeron.Counter} to represent the election state.
     */
    public int code()
    {
        return code;
    }

    /**
     * Get the enum value for a given code stored in a counter.
     *
     * @param code representing election state.
     * @return the enum value for a given code stored in a counter.
     */
    public static ElectionState get(final long code)
    {
        if (code < 0 || code > (STATES.length - 1))
        {
            throw new ClusterException("invalid election state counter code: " + code);
        }

        return STATES[(int)code];
    }

    /**
     * Get the {@link ElectionState} value based on the value stored in an {@link AtomicCounter}.
     *
     * @param counter to read the value for matching against {@link #code()}.
     * @return the {@link ElectionState} value based on the value stored in an {@link AtomicCounter}.
     */
    public static ElectionState get(final AtomicCounter counter)
    {
        if (counter.isClosed())
        {
            return CLOSED;
        }

        return get(counter.get());
    }
}
