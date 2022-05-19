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

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingSignalPoller;
import io.aeron.archive.codecs.*;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.*;
import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.media.UdpChannel;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthorisationService;
import io.aeron.status.ReadableCounter;
import org.agrona.*;
import org.agrona.collections.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.*;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.client.ReplayMerge.LIVE_ADD_MAX_WINDOW;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.ClusterSession.State.*;
import static io.aeron.cluster.ConsensusModule.Configuration.*;
import static io.aeron.cluster.client.AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS;
import static io.aeron.exceptions.AeronException.Category.WARN;
import static java.lang.Math.min;

/**
 * ConsensusModuleAgent
 *
 */
final class ConsensusModuleAgent implements Agent, TimerService.TimerHandler
{
    static final long SLOW_TICK_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final int SERVICE_MESSAGE_LIMIT = 20;
    static final short APPEND_POSITION_FLAG_NONE = 0;
    static final short APPEND_POSITION_FLAG_CATCHUP = 1;

    private final long sessionTimeoutNs;
    private final long leaderHeartbeatIntervalNs;
    private final long leaderHeartbeatTimeoutNs;
    private long unavailableCounterHandlerRegistrationId;
    private long nextSessionId = 1;
    private long nextServiceSessionId = Long.MIN_VALUE + 1;
    private long logServiceSessionId = Long.MIN_VALUE;
    private long leadershipTermId = NULL_VALUE;
    private long expectedAckPosition = 0;

    //serviceAckId, which increases once a ack message has been handled successfully,
    //By successfully, it means the ServiceAck has been reached by all services.
    private long serviceAckId = 0;

    private long terminationPosition = NULL_POSITION;
    private long notifiedCommitPosition = 0;
    private long lastAppendPosition = 0;
    private long timeOfLastLogUpdateNs = 0;
    private long timeOfLastAppendPositionUpdateNs = 0;
    private long timeOfLastAppendPositionSendNs = 0;
    private long slowTickDeadlineNs = 0;

    //periodically update markFile activityTimestamp
    private long markFileUpdateDeadlineNs = 0;
    private int pendingServiceMessageHeadOffset = 0;
    private int uncommittedServiceMessages = 0;
    private int memberId;
    private int highMemberId;

    //increases when a removeMember request is sent by ClusterTool.
    private int pendingMemberRemovals = 0;

    private long logPublicationChannelTag;

    // The position of the recording position, the recording is created based on logSessionId,
    // where logSessionId is the publication.sessionId, see: joinAsLeader
    // the recording channel has param `session-id=logSessionId` in the channel, so the recording records the log
    // publication.
    // the publication is the log publication, created when in `LEADER_LOG_REPLICATION` state
    // See: Election.LEADER_LOG_REPLICATION.
    // Use this position, together with positions from followers, the commitPosition is determined and updated,
    // and the service module and poll raft logs up until the commitPosition.
    // the position will be updated when publication.offer succeeds, the publication is created and set in `logPublisher`,
    // and publication is a mdc pub, and uses min flow control strategy if not specified.
    private ReadableCounter appendPosition = null;

    //The Raft commitPosition, this value will be read by ClusteredServiceAgent to control raft log polling.
    //This counter will be saved in aeron counters.
    private final Counter commitPosition;

    private ConsensusModule.State state = ConsensusModule.State.INIT;
    private Cluster.Role role = Cluster.Role.FOLLOWER;
    private ClusterMember[] activeMembers;
    private ClusterMember[] passiveMembers = ClusterMember.EMPTY_MEMBERS;
    private ClusterMember leaderMember;
    private ClusterMember thisMember;

    //a temporary array used for sort logPositions of cluster members
    //use this array to avoid allocation.
    private long[] rankedPositions;

    private final long[] serviceClientIds;

    //Dequeue to store ServiceAck received from service module,
    // an Ack message contains the logPosition the service module has processed.
    // the index of the serviceAckQueues array is the serviceId, which means each service has its own Dequeue.
    private final ArrayDeque<ServiceAck>[] serviceAckQueues;
    private final Counter clusterRoleCounter;

    //ClusterMarkFile, for updateActivityTimestamp in this class, update every 1 second.
    //activityTimestamp will also be updated in ClusteredServiceAgent.
    private final ClusterMarkFile markFile;

    private final AgentInvoker aeronClientInvoker;
    private final ClusterClock clusterClock;
    private final LongConsumer clusterTimeConsumer;
    private final TimeUnit clusterTimeUnit;
    private final TimerService timerService;
    private final Counter moduleState;
    private final Counter controlToggle;

    //To receive consensus related messages from service module.
    //Note: this is different from ConsensusAdapter, which responses for polling consensus messages from other nodes.
    private final ConsensusModuleAdapter consensusModuleAdapter;
    private final ServiceProxy serviceProxy;
    private final IngressAdapter ingressAdapter;
    private final EgressPublisher egressPublisher;

    //To publish log to the cluster
    private final LogPublisher logPublisher;

    //logAdapter, image will only be set if current node is follower
    //so, if current node is leader, this logAdapter is useless when logApapter.poll().
    private final LogAdapter logAdapter;

    //Adapter used for poll messages of consensus from nodes in the cluster.
    private final ConsensusAdapter consensusAdapter;

    private final ConsensusPublisher consensusPublisher = new ConsensusPublisher();
    private final Long2ObjectHashMap<ClusterSession> sessionByIdMap = new Long2ObjectHashMap<>();

    //after a client's CONNECT message is received via ingressAdapter.poll,
    // a session is created and added into `pendingSessions`,
    // the pendingSessions will be processed in `slowTickWork()`.
    private final ArrayList<ClusterSession> pendingSessions = new ArrayList<>();

    //To store all the ClientSessions that has be rejected
    //Clients will be rejected if error occurs or some other reasons, they will all be stored in rejectedSessions.
    private final ArrayList<ClusterSession> rejectedSessions = new ArrayList<>();
    //To store all the ClientSessions that have connected but need to be redirected to leader.
    private final ArrayList<ClusterSession> redirectSessions = new ArrayList<>();

    private final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();

    //Count of expired timer task.
    private final Long2LongCounterMap expiredTimerCountByCorrelationIdMap = new Long2LongCounterMap(0);

    private final ArrayDeque<ClusterSession> uncommittedClosedSessions = new ArrayDeque<>();

    private final LongArrayQueue uncommittedTimers = new LongArrayQueue(Long.MAX_VALUE);

    private final ExpandableRingBuffer pendingServiceMessages = new ExpandableRingBuffer();
    private final ExpandableRingBuffer.MessageConsumer serviceSessionMessageAppender =
        this::serviceSessionMessageAppender;
    private final ExpandableRingBuffer.MessageConsumer leaderServiceSessionMessageSweeper =
        this::leaderServiceSessionMessageSweeper;
    private final ExpandableRingBuffer.MessageConsumer followerServiceSessionMessageSweeper =
        this::followerServiceSessionMessageSweeper;
    private final Authenticator authenticator;
    private final AuthorisationService authorisationService;
    private final ClusterSessionProxy sessionProxy;
    private final Aeron aeron;
    private final ConsensusModule.Context ctx;
    private final IdleStrategy idleStrategy;
    private final RecordingLog recordingLog;
    private final ArrayList<RecordingLog.Snapshot> dynamicJoinSnapshots = new ArrayList<>();
    private RecordingLog.RecoveryPlan recoveryPlan;

    //local archive to communicate with Archive.
    private AeronArchive archive;
    //to poll recording errors and events.
    private RecordingSignalPoller recordingSignalPoller;

    private Election election;
    private DynamicJoin dynamicJoin;
    private ClusterTermination clusterTermination;

    //the raft log recording SubscriptionId.
    private long logSubscriptionId = NULL_VALUE;
    //the raft log recordingId.
    private long logRecordingId = NULL_VALUE;
    //the log recorded position, it will be assigned a non-null value only when log recording is stopped.
    private long logRecordedPosition = NULL_POSITION;

    private String liveLogDestination;
    private String catchupLogDestination;
    private String ingressEndpoints;
    private boolean isElectionRequired;

    ConsensusModuleAgent(final ConsensusModule.Context ctx)
    {
        this.ctx = ctx;
        this.aeron = ctx.aeron();
        this.clusterClock = ctx.clusterClock();
        this.clusterTimeUnit = clusterClock.timeUnit();
        this.clusterTimeConsumer = ctx.clusterTimeConsumerSupplier().apply(ctx);
        this.timerService = ctx.timerServiceSupplier().newInstance(clusterTimeUnit, this);
        this.sessionTimeoutNs = ctx.sessionTimeoutNs();
        this.leaderHeartbeatIntervalNs = ctx.leaderHeartbeatIntervalNs();
        this.leaderHeartbeatTimeoutNs = ctx.leaderHeartbeatTimeoutNs();
        this.egressPublisher = ctx.egressPublisher();
        this.moduleState = ctx.moduleStateCounter();
        this.commitPosition = ctx.commitPositionCounter();
        this.controlToggle = ctx.controlToggleCounter();
        this.logPublisher = ctx.logPublisher();
        this.idleStrategy = ctx.idleStrategy();
        this.activeMembers = ClusterMember.parse(ctx.clusterMembers());
        this.sessionProxy = new ClusterSessionProxy(egressPublisher);
        this.memberId = ctx.clusterMemberId();
        this.clusterRoleCounter = ctx.clusterNodeRoleCounter();
        this.markFile = ctx.clusterMarkFile();
        this.recordingLog = ctx.recordingLog();
        this.serviceClientIds = new long[ctx.serviceCount()];
        Arrays.fill(serviceClientIds, NULL_VALUE);
        this.serviceAckQueues = ServiceAck.newArrayOfQueues(ctx.serviceCount());
        this.highMemberId = ClusterMember.highMemberId(activeMembers);

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
        role(Cluster.Role.FOLLOWER);

        ClusterMember.addClusterMemberIds(activeMembers, clusterMemberByIdMap);
        thisMember = ClusterMember.determineMember(activeMembers, ctx.clusterMemberId(), ctx.memberEndpoints());
        leaderMember = thisMember;

        //the channel parameter for the consensus communication channel.
        final ChannelUri consensusUri = ChannelUri.parse(ctx.consensusChannel());
        if (!consensusUri.containsKey(ENDPOINT_PARAM_NAME))
        {
            consensusUri.put(ENDPOINT_PARAM_NAME, thisMember.consensusEndpoint());
        }

        //To receive consensus related messages from others nodes.
        consensusAdapter = new ConsensusAdapter(
            aeron.addSubscription(consensusUri.toString(), ctx.consensusStreamId()), this);

        //IngressAdapter, used for receive client session requests and messages.
        ingressAdapter = new IngressAdapter(ctx.ingressFragmentLimit(), this);

        //only useful when current node is follower. poll raft log from leader.
        logAdapter = new LogAdapter(this, ctx.logFragmentLimit());

        //To receive consensus module related messages from service module.
        consensusModuleAdapter = new ConsensusModuleAdapter(
            aeron.addSubscription(ctx.controlChannel(), ctx.consensusModuleStreamId()), this);
        serviceProxy = new ServiceProxy(aeron.addPublication(ctx.controlChannel(), ctx.serviceStreamId()));

        authenticator = ctx.authenticatorSupplier().get();
        authorisationService = ctx.authorisationServiceSupplier().get();
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        if (!aeron.isClosed())
        {
            aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);
            tryStopLogRecording();

            if (!ctx.ownsAeronClient())
            {
                logPublisher.disconnect(ctx.countedErrorHandler());
                logAdapter.disconnect(ctx.countedErrorHandler());

                final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
                for (final ClusterSession session : sessionByIdMap.values())
                {
                    session.close(aeron, errorHandler);
                }

                CloseHelper.close(errorHandler, ingressAdapter);
                ClusterMember.closeConsensusPublications(errorHandler, activeMembers);
                CloseHelper.close(errorHandler, consensusAdapter);
                CloseHelper.close(errorHandler, serviceProxy);
                CloseHelper.close(errorHandler, consensusModuleAdapter);
                CloseHelper.close(errorHandler, archive);
            }

            state(ConsensusModule.State.CLOSED);
        }

        markFile.updateActivityTimestamp(NULL_VALUE);
        ctx.close();
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        //create local archive
        archive = AeronArchive.connect(ctx.archiveContext().clone());
        //create RecordingSignalPoller
        recordingSignalPoller = new RecordingSignalPoller(
            archive.controlSessionId(), archive.controlResponsePoller().subscription());

        if (null == (dynamicJoin = requiresDynamicJoin()))
        {
            final long lastTermRecordingId = recordingLog.findLastTermRecordingId();
            if (NULL_VALUE != lastTermRecordingId)
            {
                archive.tryStopRecordingByIdentity(lastTermRecordingId);
            }

            recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount(), logRecordingId);
            if (null != recoveryPlan.log)
            {
                logRecordingId = recoveryPlan.log.recordingId;
            }

            try (Counter counter = addRecoveryStateCounter(recoveryPlan))
            {
                assert null != counter;

                if (!recoveryPlan.snapshots.isEmpty())
                {
                    loadSnapshot(recoveryPlan.snapshots.get(0), archive);
                }

                while (!ServiceAck.hasReached(expectedAckPosition, serviceAckId, serviceAckQueues))
                {
                    idle(consensusModuleAdapter.poll());
                }

                captureServiceClientIds();
                ++serviceAckId;
            }

            ClusterMember.addConsensusPublications(
                activeMembers,
                thisMember,
                ctx.consensusChannel(),
                ctx.consensusStreamId(),
                aeron,
                ctx.countedErrorHandler());

            election = new Election(
                true,
                recoveryPlan.lastLeadershipTermId,
                commitPosition.getWeak(),
                recoveryPlan.appendedLogPosition,
                activeMembers,
                clusterMemberByIdMap,
                thisMember,
                consensusPublisher,
                ctx,
                this);

            election.doWork(clusterClock.timeNanos());
            state(ConsensusModule.State.ACTIVE);
        }

        unavailableCounterHandlerRegistrationId = aeron.addUnavailableCounterHandler(this::onUnavailableCounter);
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        //get current system timestamp.
        final long timestamp = clusterClock.time();
        final long nowNs = clusterTimeUnit.toNanos(timestamp);
        int workCount = 0;

        try
        {
            //invoke every 10ms
            if (nowNs >= slowTickDeadlineNs)
            {
                slowTickDeadlineNs = nowNs + SLOW_TICK_INTERVAL_NS;
                workCount += slowTickWork(nowNs);
            }

            //poll consensusAdapter to handle election related messages between nodes.
            workCount += consensusAdapter.poll();

            if (null != dynamicJoin)
            {
                workCount += dynamicJoin.doWork(nowNs);
            }
            else if (null != election)
            {
                //If election is in progress, do election work
                workCount += election.doWork(nowNs);
            }
            else
            {
                workCount += consensusWork(timestamp, nowNs);
            }
        }
        catch (final AgentTerminationException ex)
        {
            runTerminationHook();
            throw ex;
        }
        catch (final Exception ex)
        {
            if (null != election)
            {
                election.handleError(nowNs, ex);
            }
            else
            {
                throw ex;
            }
        }

        clusterTimeConsumer.accept(timestamp);

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        final String agentRoleName = ctx.agentRoleName();
        return null != agentRoleName ? agentRoleName : "consensus-module_" + ctx.clusterId() + "_" + memberId;
    }

    //Called back from IngressAdapter.poll
    //invoked when a CONNECT message is received from a client.
    void onSessionConnect(
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        //assign clusterSessionId, if current node is not leader, then assign -1.
        final long clusterSessionId = Cluster.Role.LEADER == role ? nextSessionId++ : NULL_VALUE;

        final ClusterSession session = new ClusterSession(
            clusterSessionId, responseStreamId, createResponseChannel(responseChannel));

        //Async connect the session, but when the publication is ready, the image will be created in `doWork()` loop,
        //in `aeronClientInvoker.invoke()` which takes time.
        session.asyncConnect(aeron);

        final long now = clusterClock.time();
        session.lastActivityNs(clusterTimeUnit.toNanos(now), correlationId);

        //If current node is not leader, add the session into redirect collections, and redirect them to leader later.
        if (Cluster.Role.LEADER != role)
        {
            redirectSessions.add(session);
        }
        else
        {
            //check if protocol version works well with client version,
            // if not, add session into rejected sessions with protocol version error messages.
            if (AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION != SemanticVersion.major(version))
            {
                final String detail = SESSION_INVALID_VERSION_MSG + " " + SemanticVersion.toString(version) +
                    ", cluster is " + SemanticVersion.toString(PROTOCOL_SEMANTIC_VERSION);
                session.reject(EventCode.ERROR, detail);
                rejectedSessions.add(session);
            }
            //if total session size overflows, add the session into rejected with session limit messages.
            else if (pendingSessions.size() + sessionByIdMap.size() >= ctx.maxConcurrentSessions())
            {
                session.reject(EventCode.ERROR, SESSION_LIMIT_MSG);
                rejectedSessions.add(session);
            }
            else
            {
                authenticator.onConnectRequest(session.id(), encodedCredentials, clusterTimeUnit.toMillis(now));

                //add session into pending sessions and wait to be handled in `consensusWork()`
                pendingSessions.add(session);
            }
        }
    }

    //Called back from IngressAdapter.poll
    //invoked when a CLose message is received from clients.
    void onSessionClose(final long leadershipTermId, final long clusterSessionId)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.state() == OPEN)
            {
                //mark session as closed with reason: CLIENT_ACTION.
                session.closing(CloseReason.CLIENT_ACTION);

                //disconnect, which will remove publication from aeron and close.
                session.disconnect(aeron, ctx.countedErrorHandler());

                //append SessionClose message into raft log.
                if (logPublisher.appendSessionClose(
                    memberId, session, leadershipTermId, clusterClock.time(), clusterClock.timeUnit()))
                {
                    session.closedLogPosition(logPublisher.position());
                    uncommittedClosedSessions.addLast(session);
                    sessionByIdMap.remove(clusterSessionId);
                    session.close(aeron, ctx.countedErrorHandler());
                }
            }
        }
    }

    //Called back from IngressAdapter.poll
    //invoked when a AdminRequest message is received from clients.
    //There is only one type of Admin action for now: SNAPSHOT.
    void onAdminRequest(
        final long leadershipTermId,
        final long clusterSessionId,
        final long correlationId,
        final AdminRequestType requestType,
        final DirectBuffer payload,
        final int payloadOffset,
        final int payloadLength)
    {
        if (Cluster.Role.LEADER != role)
        {
            return;
        }

        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null == session || session.state() != OPEN)
        {
            return;
        }

        if (leadershipTermId != this.leadershipTermId)
        {
            final String msg =
                "Invalid leadership term: expected " + this.leadershipTermId + ", got " + leadershipTermId;
            egressPublisher.sendAdminResponse(session, correlationId, requestType, AdminResponseCode.ERROR, msg);
            return;
        }

        if (!authorisationService.isAuthorised(
            MessageHeaderDecoder.SCHEMA_ID, AdminRequestDecoder.TEMPLATE_ID, requestType, session.encodedPrincipal()))
        {
            final String msg = "Execution of the " + requestType + " request was not authorised";
            egressPublisher.sendAdminResponse(
                session, correlationId, requestType, AdminResponseCode.UNAUTHORISED_ACCESS, msg);
            return;
        }

        if (AdminRequestType.SNAPSHOT == requestType)
        {
            //Control toggleState, if successfully, response with ok, else response with error.
            //TODO: how ToggleState works.
            if (ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle))
            {
                egressPublisher.sendAdminResponse(session, correlationId, requestType, AdminResponseCode.OK, "");
            }
            else
            {
                final String msg = "Failed to switch ClusterControl to the ToggleState.SNAPSHOT state";
                egressPublisher.sendAdminResponse(session, correlationId, requestType, AdminResponseCode.ERROR, msg);
            }
        }
        else
        {
            egressPublisher.sendAdminResponse(
                session, correlationId, requestType, AdminResponseCode.ERROR, "Unknown request type: " + requestType);
        }
    }

    //Called back from IngressAdapter
    //invoked when a CLOSE message is received.
    ControlledFragmentAssembler.Action onIngressMessage(
        final long leadershipTermId,   //leader termId, which means the client also keep tracking of the leader termId.
        final long clusterSessionId,   //the sessionId for client connection.
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {

        // handle client messages only when current node is leader and its leaderTermId matches the one from clients.
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.state() == OPEN)
            {
                // Use current timestamp as the timestamp. The timestamp is the one that passes into
                // ClusterService.onSessionMessage(timestamp).
                final long timestamp = clusterClock.time();
                if (logPublisher.appendMessage(
                    leadershipTermId, clusterSessionId, timestamp, buffer, offset, length) > 0)
                {
                    session.timeOfLastActivityNs(clusterTimeUnit.toNanos(timestamp));
                    return ControlledFragmentHandler.Action.CONTINUE;
                }
                else
                {
                    //if append failed, return ABORT, so the message will be waiting for the next poll.
                    return ControlledFragmentHandler.Action.ABORT;
                }
            }
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    //Called back from IngressAdapter
    //invoked when a KeepAlive message is received from a client.
    void onSessionKeepAlive(final long leadershipTermId, final long clusterSessionId)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterSession session = sessionByIdMap.get(clusterSessionId);
            if (null != session && session.state() == OPEN)
            {
                //update session activity with current timestamp in nanosecond.
                session.timeOfLastActivityNs(clusterClock.timeNanos());
            }
        }
    }

    //Called back from IngressAdapter
    //invoked when a ChallengeResponse message is received from a client.
    void onChallengeResponse(final long correlationId, final long clusterSessionId, final byte[] encodedCredentials)
    {
        if (Cluster.Role.LEADER == role)
        {
            for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
            {
                final ClusterSession session = pendingSessions.get(i);

                if (session.id() == clusterSessionId && session.state() == CHALLENGED)
                {
                    final long timestamp = clusterClock.time();
                    final long nowMs = clusterTimeUnit.toMillis(timestamp);
                    session.lastActivityNs(clusterTimeUnit.toNanos(timestamp), correlationId);
                    authenticator.onChallengeResponse(clusterSessionId, encodedCredentials, nowMs);
                    break;
                }
            }
        }
    }

    /**
     * Called back from TimerService which will be called from `consensusWork()`
     * This method will be called when a timer task should be invoked.
     * @param correlationId of the timer.  This correlationId is the one that triggers the timer.
     * @return true if successfully append it to logs otherwise false.
     *  If true is returned, this task will be removed, otherwise it will be retried.
     */
    public boolean onTimerEvent(final long correlationId)
    {
        //Append timer into logs, the timestamp is retrieved from `clusterClock.time()`
        final long appendPosition = logPublisher.appendTimer(correlationId, leadershipTermId, clusterClock.time());
        if (appendPosition > 0)
        {
            uncommittedTimers.offerLong(appendPosition);
            uncommittedTimers.offerLong(correlationId);
            return true;
        }

        return false;
    }

    //Called back from ConsensusAdapter
    // To handle CanvassPosition messages, and create a publication if the follower does not have one.
    // CANVASS: The node is checking connectivity between nodes, which is used to ensure a successful election is possible.
    // This means a follower is canvassing for leadership.
    void onCanvassPosition(
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId)
    {
        //if the follower does not have a publication connected then add a publication to the follower.
        checkFollowerForConsensusPublication(followerMemberId);

        if (null != election)
        {
            election.onCanvassPosition(logLeadershipTermId, logPosition, leadershipTermId, followerMemberId);
        }
        else if (Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            //if logLeadershipTermId from follower is less than or equal to current leadershipTermId
            //send a newLeadershipTerm message to this follower with its own logLeadershipTermId.
            if (null != follower && logLeadershipTermId <= this.leadershipTermId)
            {
                final RecordingLog.Entry currentTermEntry = recordingLog.getTermEntry(this.leadershipTermId);
                final long termBaseLogPosition = currentTermEntry.termBaseLogPosition;
                final long nextLogLeadershipTermId;
                final long nextTermBaseLogPosition;
                final long nextLogPosition;

                if (logLeadershipTermId < this.leadershipTermId)
                {
                    final RecordingLog.Entry nextLogEntry = recordingLog.findTermEntry(logLeadershipTermId + 1);
                    nextLogLeadershipTermId = null != nextLogEntry ?
                        nextLogEntry.leadershipTermId : this.leadershipTermId;
                    nextTermBaseLogPosition = null != nextLogEntry ?
                        nextLogEntry.termBaseLogPosition : termBaseLogPosition;
                    nextLogPosition = null != nextLogEntry ? nextLogEntry.logPosition : NULL_POSITION;
                }
                else
                {
                    nextLogLeadershipTermId = NULL_VALUE;
                    nextTermBaseLogPosition = NULL_POSITION;
                    nextLogPosition = NULL_POSITION;
                }

                //TODO: why use logLeadershipTermId instead of this.leadershipTermId ?
                consensusPublisher.newLeadershipTerm(
                    follower.publication(),
                    logLeadershipTermId,
                    nextLogLeadershipTermId,
                    nextTermBaseLogPosition,
                    nextLogPosition,
                    this.leadershipTermId,
                    termBaseLogPosition,
                    logPublisher.position(),
                    logRecordingId,
                    clusterClock.time(),
                    memberId,
                    logPublisher.sessionId(),
                    false);
            }
        }
    }

    /**
     * Called back from ConsensusAdapter
     * Requested by a candidate to vote for its leadership.
     * @param logLeadershipTermId
     * @param logPosition
     * @param candidateTermId  termId by the candidate.
     * @param candidateId
     */
    void onRequestVote(
        final long logLeadershipTermId, final long logPosition, final long candidateTermId, final int candidateId)
    {
        //If an election is in progress, handle in the election process.
        //If candidateTermId is greater than current leadershipTermId, then enter election stage.
        if (null != election)
        {
            election.onRequestVote(logLeadershipTermId, logPosition, candidateTermId, candidateId);
        }
        else if (candidateTermId > leadershipTermId && null == dynamicJoin)
        {
            ctx.countedErrorHandler().onError(new ClusterEvent("unexpected vote request"));
            enterElection();
        }
    }

    //Called back from ConsensusAdapter.
    //invoked when a RequestVote response is received.
    void onVote(
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        if (null != election)
        {
            election.onVote(
                candidateTermId, logLeadershipTermId, logPosition, candidateMemberId, followerMemberId, vote);
        }
    }

    //Called back from ConsensusAdapter.
    //A leader has been successfully elected and has begun a new term.
    void onNewLeadershipTerm(
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderId,
        final int logSessionId,
        final boolean isStartup)
    {
        logNewLeadershipTerm(
            logLeadershipTermId,
            nextLeadershipTermId,
            nextTermBaseLogPosition,
            nextLogPosition,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            leaderRecordingId,
            timestamp,
            memberId,
            leaderId,
            logSessionId,
            isStartup);

        if (null != election)
        {
            election.onNewLeadershipTerm(
                logLeadershipTermId,
                nextLeadershipTermId,
                nextTermBaseLogPosition,
                nextLogPosition,
                leadershipTermId,
                termBaseLogPosition,
                logPosition,
                leaderRecordingId,
                timestamp,
                leaderId,
                logSessionId,
                isStartup);
        }
        else if (Cluster.Role.FOLLOWER == role &&
            leadershipTermId == this.leadershipTermId &&
            leaderId == leaderMember.id())
        {
            notifiedCommitPosition = Math.max(notifiedCommitPosition, logPosition);
            timeOfLastLogUpdateNs = clusterClock.timeNanos();
        }
        else if (leadershipTermId > this.leadershipTermId && null == dynamicJoin)
        {
            ctx.countedErrorHandler().onError(new ClusterEvent("unexpected new leadership term event"));
            enterElection();
        }
    }

    void onAppendPosition(
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
        if (null != election)
        {
            election.onAppendPosition(leadershipTermId, logPosition, followerMemberId, flags);
        }
        else if (leadershipTermId <= this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower)
            {
                follower
                    .logPosition(logPosition)
                    .timeOfLastAppendPositionNs(clusterClock.timeNanos());
                trackCatchupCompletion(follower, leadershipTermId, flags);
            }
        }
    }

    void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        logCommitPosition(leadershipTermId, logPosition, leaderMemberId, memberId);

        if (null != election)
        {
            election.onCommitPosition(leadershipTermId, logPosition, leaderMemberId);
        }
        else if (leadershipTermId == this.leadershipTermId &&
            leaderMemberId == leaderMember.id() &&
            Cluster.Role.FOLLOWER == role)
        {
            notifiedCommitPosition = logPosition;
            timeOfLastLogUpdateNs = clusterClock.timeNanos();
        }
        else if (leadershipTermId > this.leadershipTermId && null == dynamicJoin)
        {
            ctx.countedErrorHandler().onError(new ClusterEvent("unexpected commit position from new leader"));
            enterElection();
        }
    }

    void onCatchupPosition(
        final long leadershipTermId, final long logPosition, final int followerMemberId, final String catchupEndpoint)
    {
        if (leadershipTermId <= this.leadershipTermId && Cluster.Role.LEADER == role)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower && follower.catchupReplaySessionId() == NULL_VALUE)
            {
                final ChannelUri channel = ChannelUri.parse(ctx.followerCatchupChannel());
                channel.put(ENDPOINT_PARAM_NAME, catchupEndpoint);
                channel.put(SESSION_ID_PARAM_NAME, Integer.toString(logPublisher.sessionId()));
                channel.put(LINGER_PARAM_NAME, "0");
                channel.put(EOS_PARAM_NAME, "false");

                follower.catchupReplaySessionId(archive.startReplay(
                    logRecordingId, logPosition, Long.MAX_VALUE, channel.toString(), ctx.logStreamId()));
                follower.catchupReplayCorrelationId(archive.lastCorrelationId());
            }
        }
    }

    void onStopCatchup(final long leadershipTermId, final int followerMemberId)
    {
        if (leadershipTermId == this.leadershipTermId && followerMemberId == memberId)
        {
            if (null != catchupLogDestination)
            {
                logAdapter.removeDestination(catchupLogDestination);
                catchupLogDestination = null;
            }
        }
    }

    void onAddPassiveMember(final long correlationId, final String memberEndpoints)
    {
        logAddPassiveMember(correlationId, memberEndpoints, memberId);

        if (null == election && null == dynamicJoin)
        {
            if (Cluster.Role.LEADER == role)
            {
                if (ClusterMember.notDuplicateEndpoint(passiveMembers, memberEndpoints) &&
                    ClusterMember.notDuplicateEndpoint(activeMembers, memberEndpoints))
                {
                    final ClusterMember newMember = ClusterMember.parseEndpoints(++highMemberId, memberEndpoints);

                    newMember.correlationId(correlationId);
                    passiveMembers = ClusterMember.addMember(passiveMembers, newMember);
                    clusterMemberByIdMap.put(newMember.id(), newMember);

                    ClusterMember.addConsensusPublication(
                        newMember, ctx.consensusChannel(), ctx.consensusStreamId(), aeron, ctx.countedErrorHandler());
                    logPublisher.addDestination(ctx.isLogMdc(), newMember.logEndpoint());
                }
            }
            else if (Cluster.Role.FOLLOWER == role)
            {
                consensusPublisher.addPassiveMember(leaderMember.publication(), correlationId, memberEndpoints);
            }
        }
    }

    void onClusterMembersChange(
        final long correlationId, final int leaderMemberId, final String activeMembers, final String passiveMembers)
    {
        if (null != dynamicJoin)
        {
            dynamicJoin.onClusterMembersChange(correlationId, leaderMemberId, activeMembers, passiveMembers);
        }
    }

    void onSnapshotRecordingQuery(final long correlationId, final int requestMemberId)
    {
        if (null == election && Cluster.Role.LEADER == role)
        {
            final ClusterMember requester = clusterMemberByIdMap.get(requestMemberId);
            if (null != requester)
            {
                consensusPublisher.snapshotRecording(
                    requester.publication(),
                    correlationId,
                    recoveryPlan,
                    ClusterMember.encodeAsString(activeMembers));
            }
        }
    }

    void onSnapshotRecordings(final long correlationId, final SnapshotRecordingsDecoder decoder)
    {
        if (null != dynamicJoin)
        {
            dynamicJoin.onSnapshotRecordings(correlationId, decoder);
        }
    }

    void onJoinCluster(final long leadershipTermId, final int memberId)
    {
        if (null == election && Cluster.Role.LEADER == role)
        {
            final ClusterMember member = clusterMemberByIdMap.get(memberId);
            final long snapshotLeadershipTermId = recoveryPlan.snapshots.isEmpty() ?
                NULL_VALUE : recoveryPlan.snapshots.get(0).leadershipTermId;

            if (null != member && !member.hasRequestedJoin() && leadershipTermId <= snapshotLeadershipTermId)
            {
                if (null == member.publication())
                {
                    ClusterMember.addConsensusPublication(
                        member, ctx.consensusChannel(), ctx.consensusStreamId(), aeron, ctx.countedErrorHandler());
                    logPublisher.addDestination(ctx.isLogMdc(), member.logEndpoint());
                }

                member.hasRequestedJoin(true);
            }
        }
    }

    void onTerminationPosition(final long leadershipTermId, final long logPosition)
    {
        if (leadershipTermId == this.leadershipTermId && Cluster.Role.FOLLOWER == role)
        {
            terminationPosition = logPosition;
            timeOfLastLogUpdateNs = clusterClock.timeNanos();
        }
    }

    void onTerminationAck(final long leadershipTermId, final long logPosition, final int memberId)
    {
        if (leadershipTermId == this.leadershipTermId &&
            logPosition >= terminationPosition &&
            Cluster.Role.LEADER == role)
        {
            final ClusterMember member = clusterMemberByIdMap.get(memberId);
            if (null != member)
            {
                member.hasTerminated(true);

                if (clusterTermination.canTerminate(activeMembers, terminationPosition, clusterClock.timeNanos()))
                {
                    recordingLog.commitLogPosition(leadershipTermId, terminationPosition);
                    closeAndTerminate();
                }
            }
        }
    }

    void onBackupQuery(
        final long correlationId,
        final int responseStreamId,
        final int version,
        final String responseChannel,
        final byte[] encodedCredentials)
    {
        //works only when there is no election or dynamicJoin in progress.
        if (null == election && null == dynamicJoin)
        {
            if (Cluster.Role.LEADER != role)
            {
                consensusPublisher.backupQuery(
                    leaderMember.publication(),
                    correlationId,
                    responseStreamId,
                    version,
                    responseChannel,
                    encodedCredentials);
            }
            else if (state == ConsensusModule.State.ACTIVE || state == ConsensusModule.State.SUSPENDED)
            {
                final ClusterSession session = new ClusterSession(
                    NULL_VALUE, responseStreamId, createResponseChannel(responseChannel));

                session.markAsBackupSession();
                session.asyncConnect(aeron);

                final long timestamp = clusterClock.time();
                session.lastActivityNs(clusterTimeUnit.toNanos(timestamp), correlationId);

                if (AeronCluster.Configuration.PROTOCOL_MAJOR_VERSION != SemanticVersion.major(version))
                {
                    final String detail = SESSION_INVALID_VERSION_MSG + " " + SemanticVersion.toString(version) +
                        ", cluster=" + SemanticVersion.toString(PROTOCOL_SEMANTIC_VERSION);
                    session.reject(EventCode.ERROR, detail);
                    rejectedSessions.add(session);
                }
                else
                {
                    authenticator.onConnectRequest(
                        session.id(), encodedCredentials, clusterTimeUnit.toMillis(timestamp));
                    pendingSessions.add(session);
                }
            }
        }
    }

    //Called back from ConsensusModuleAdapter to remove a member from cluster.
    //This is invoked form `ClusterTool.removeMember()`
    void onRemoveMember(final int memberId, final boolean isPassive)
    {
        //works only when current node is leader and there is no election in progress.
        if (null == election && Cluster.Role.LEADER == role)
        {
            //works only when the member is found by memberId.
            final ClusterMember member = clusterMemberByIdMap.get(memberId);
            if (null != member)
            {
                if (isPassive)
                {
                    passiveMembers = ClusterMember.removeMember(passiveMembers, memberId);
                    member.closePublication(ctx.countedErrorHandler());

                    logPublisher.removeDestination(ctx.isLogMdc(), member.logEndpoint());

                    clusterMemberByIdMap.remove(memberId);
                    clusterMemberByIdMap.compact();
                }
                else
                {
                    //If not passive member
                    //append event to raft log
                    final long now = clusterClock.time();
                    final long position = logPublisher.appendMembershipChangeEvent(
                        leadershipTermId,
                        now,
                        this.memberId,
                        activeMembers.length,
                        ChangeType.QUIT,
                        memberId,
                        ClusterMember.encodeAsString(ClusterMember.removeMember(activeMembers, memberId)));

                    //If append successfully, update member's removalPosition and increase pendingMemberRemovals.
                    //If append failed, then there will be no retrying.
                    //The real removal action is in `updateLeaderPosition`
                    if (position > 0)
                    {
                        timeOfLastLogUpdateNs = clusterTimeUnit.toNanos(now) - leaderHeartbeatIntervalNs;
                        member.removalPosition(position);
                        pendingMemberRemovals++;
                    }
                }
            }
        }
    }

    //Called back from ConsensusModuleAdapter to query cluster members.
    //This is invoked from `ClusterTool.queryClusterMembers()`
    void onClusterMembersQuery(final long correlationId, final boolean isExtendedRequest)
    {
        //Default is true
        if (isExtendedRequest)
        {
            //send responses to service via ServiceProxy
            serviceProxy.clusterMembersExtendedResponse(
                correlationId, clusterClock.timeNanos(), leaderMember.id(), memberId, activeMembers, passiveMembers);
        }
        else
        {
            serviceProxy.clusterMembersResponse(
                correlationId,
                leaderMember.id(),
                ClusterMember.encodeAsString(activeMembers),
                ClusterMember.encodeAsString(passiveMembers));
        }
    }

    void state(final ConsensusModule.State newState)
    {
        if (newState != state)
        {
            logStateChange(state, newState, memberId);
            state = newState;
            if (!moduleState.isClosed())
            {
                moduleState.set(newState.code());
            }
        }
    }

    ConsensusModule.State state()
    {
        return state;
    }

    private void logStateChange(
        final ConsensusModule.State oldState, final ConsensusModule.State newState, final int memberId)
    {
        //System.out.println("CM State memberId=" + memberId + " " + oldState + " -> " + newState);
    }

    void role(final Cluster.Role newRole)
    {
        if (newRole != role)
        {
            logRoleChange(role, newRole, memberId);
            role = newRole;
            if (!clusterRoleCounter.isClosed())
            {
                clusterRoleCounter.set(newRole.code());
            }
        }
    }

    private void logRoleChange(final Cluster.Role oldRole, final Cluster.Role newRole, final int memberId)
    {
        //System.out.println("CM Role memberId=" + memberId + " " + oldRole + " -> " + newRole);
    }

    Cluster.Role role()
    {
        return role;
    }

    long prepareForNewLeadership(final long logPosition, final long nowNs)
    {
        role(Cluster.Role.FOLLOWER);
        CloseHelper.close(ctx.countedErrorHandler(), ingressAdapter);
        ClusterControl.ToggleState.deactivate(controlToggle);

        if (null != catchupLogDestination)
        {
            logAdapter.removeDestination(catchupLogDestination);
            catchupLogDestination = null;
        }

        if (null != liveLogDestination)
        {
            logAdapter.removeDestination(liveLogDestination);
            liveLogDestination = null;
        }

        logAdapter.disconnect(ctx.countedErrorHandler());
        logPublisher.disconnect(ctx.countedErrorHandler());

        if (RecordingPos.NULL_RECORDING_ID != logRecordingId)
        {
            tryStopLogRecording();
            lastAppendPosition = getLastAppendedPosition();
            timeOfLastAppendPositionUpdateNs = nowNs;
            recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount(), logRecordingId);

            final CountersReader counters = ctx.aeron().countersReader();
            while (CountersReader.NULL_COUNTER_ID != RecordingPos.findCounterIdByRecording(counters, logRecordingId))
            {
                idle();
            }

            clearSessionsAfter(logPosition);
            for (final ClusterSession session : sessionByIdMap.values())
            {
                session.disconnect(aeron, ctx.countedErrorHandler());
            }

            commitPosition.setOrdered(logPosition);
            restoreUncommittedEntries(logPosition);
        }

        return lastAppendPosition;
    }

    //Called back from ConsensusModuleAdapter
    //Invoked only when `ClusteredServiceAgent.close()` method is called, in which all client sessions will be close.
    void onServiceCloseSession(final long clusterSessionId)
    {
        final ClusterSession session = sessionByIdMap.get(clusterSessionId);
        if (null != session)
        {
            //update close reason
            session.closing(CloseReason.SERVICE_ACTION);

            //if current node is leader,
            //and appendSessionClose message successfully
            if (Cluster.Role.LEADER == role && logPublisher.appendSessionClose(
                memberId, session, leadershipTermId, clusterClock.time(), clusterClock.timeUnit()))
            {
                //Sent close event to the session, with SERVICE_ACTION message.
                final String msg = CloseReason.SERVICE_ACTION.name();
                egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                 //update closed log position
                session.closedLogPosition(logPublisher.position());

                //add current session into uncommittedClosedSessions for latter process.
                uncommittedClosedSessions.addLast(session);
                //remove session from the msp.
                sessionByIdMap.remove(clusterSessionId);
                //remove related publication.
                session.close(aeron, ctx.countedErrorHandler());
            }
        }
    }

    //Called back from ConsensusModuleAdapter,
    //invoked when a message is offered from ClusterService.
    void onServiceMessage(final long leadershipTermId, final DirectBuffer buffer, final int offset, final int length)
    {
        if (leadershipTermId == this.leadershipTermId)
        {
            //append this message into pendingServiceMessages, throws exception if capacity limit is reached.
            enqueueServiceSessionMessage((MutableDirectBuffer)buffer, offset, length, nextServiceSessionId++);
        }
    }

    //Called back from ConsensusModuleAdapter and ConsensusModuleSnapshotLoader
    //invoked when scheduleTimer is called from Cluster or snapshot.
    void onScheduleTimer(final long correlationId, final long deadline)
    {
        //If there is no expired timer task in current correlationId, schedule the task in timerService.
        if (expiredTimerCountByCorrelationIdMap.get(correlationId) == 0)
        {
            timerService.scheduleTimerForCorrelationId(correlationId, deadline);
        }
        else
        {
            // decrease the expired timer task count, but do not schedule the task.
            // todo: why ?
            expiredTimerCountByCorrelationIdMap.decrementAndGet(correlationId);
        }
    }

    //Called back from ConsensusModuleAdapter
    //invoked when cancelTimer is called from Cluster.
    void onCancelTimer(final long correlationId)
    {
        //just delegate to timerService.
        timerService.cancelTimerByCorrelationId(correlationId);
    }

    //Called back from ConsensusModuleAdapter
    //invoked when an ACK is sent from service module to acknowledge its logPosition.
    void onServiceAck(
        final long logPosition, final long timestamp, final long ackId, final long relevantId, final int serviceId)
    {
        //store the ServiceAck into ArrayDequeue.
        captureServiceAck(logPosition, ackId, relevantId, serviceId);

        //Check if all the services has acknowledged its logPosition.
        if (ServiceAck.hasReached(logPosition, serviceAckId, serviceAckQueues))
        {

            //If current is in SNAPSHOT state, and service module has finished snapshot and sent back ack.
            if (ConsensusModule.State.SNAPSHOT == state)
            {
                final ServiceAck[] serviceAcks = pollServiceAcks(logPosition, serviceId);
                ++serviceAckId;

                //take snapshot for raft log
                takeSnapshot(timestamp, logPosition, serviceAcks);

                if (null != clusterTermination)
                {
                    serviceProxy.terminationPosition(terminationPosition, ctx.countedErrorHandler());
                    clusterTermination.deadlineNs(clusterClock.timeNanos() + ctx.terminationTimeoutNs());
                    state(ConsensusModule.State.TERMINATING);
                }
                else
                {
                    state(ConsensusModule.State.ACTIVE);
                    if (Cluster.Role.LEADER == role)
                    {
                        ClusterControl.ToggleState.reset(controlToggle);
                    }
                }
            }
            else if (ConsensusModule.State.QUITTING == state)
            {
                closeAndTerminate();
            }
            else if (ConsensusModule.State.TERMINATING == state)
            {
                if (null == clusterTermination)
                {
                    consensusPublisher.terminationAck(
                        leaderMember.publication(), leadershipTermId, logPosition, memberId);
                    recordingLog.commitLogPosition(leadershipTermId, logPosition);
                    closeAndTerminate();
                }
                else
                {
                    clusterTermination.onServicesTerminated();
                    if (clusterTermination.canTerminate(
                        activeMembers, terminationPosition, clusterClock.timeNanos()))
                    {
                        recordingLog.commitLogPosition(leadershipTermId, logPosition);
                        closeAndTerminate();
                    }
                }
            }
        }
    }

    void onReplaySessionMessage(final long clusterSessionId, final long timestamp)
    {
        final ClusterSession clusterSession = sessionByIdMap.get(clusterSessionId);
        if (null == clusterSession)
        {
            logServiceSessionId = clusterSessionId;
            pendingServiceMessages.consume(followerServiceSessionMessageSweeper, Integer.MAX_VALUE);
        }
        else
        {
            clusterSession.timeOfLastActivityNs(clusterTimeUnit.toNanos(timestamp));
        }
    }

    //Called back from LogAdapter.
    //todo
    void onReplayTimerEvent(final long correlationId)
    {
        if (!timerService.cancelTimerByCorrelationId(correlationId))
        {
            expiredTimerCountByCorrelationIdMap.getAndIncrement(correlationId);
        }
    }

    void onReplaySessionOpen(
        final long logPosition,
        final long correlationId,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel)
    {
        final ClusterSession session = new ClusterSession(
            clusterSessionId, responseStreamId, createResponseChannel(responseChannel));
        session.open(logPosition);
        session.lastActivityNs(clusterTimeUnit.toNanos(timestamp), correlationId);

        sessionByIdMap.put(clusterSessionId, session);
        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
        }
    }

    void onReplaySessionClose(final long clusterSessionId, final CloseReason closeReason)
    {
        final ClusterSession clusterSession = sessionByIdMap.remove(clusterSessionId);
        if (null != clusterSession)
        {
            clusterSession.closing(closeReason);
            clusterSession.close(aeron, ctx.countedErrorHandler());
        }
    }

    void onReplayClusterAction(final long leadershipTermId, final ClusterAction action)
    {
        if (leadershipTermId == this.leadershipTermId)
        {
            if (ClusterAction.SUSPEND == action)
            {
                state(ConsensusModule.State.SUSPENDED);
            }
            else if (ClusterAction.RESUME == action)
            {
                state(ConsensusModule.State.ACTIVE);
            }
            else if (ClusterAction.SNAPSHOT == action)
            {
                state(ConsensusModule.State.SNAPSHOT);
            }
        }
    }

    void onReplayNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        logReplayNewLeadershipTermEvent(
            memberId,
            null != election,
            leadershipTermId,
            logPosition,
            timestamp,
            termBaseLogPosition,
            timeUnit,
            appVersion);

        if (timeUnit != clusterTimeUnit)
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible timestamp units: " + clusterTimeUnit + " log=" + timeUnit,
                AeronException.Category.FATAL));
            unexpectedTermination();
        }

        if (SemanticVersion.major(ctx.appVersion()) != SemanticVersion.major(appVersion))
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " log=" + SemanticVersion.toString(appVersion),
                AeronException.Category.FATAL));
            unexpectedTermination();
        }

        this.leadershipTermId = leadershipTermId;

        if (null != election)
        {
            election.onReplayNewLeadershipTermEvent(leadershipTermId, logPosition, timestamp, termBaseLogPosition);
        }
    }

    void onReplayMembershipChange(
        final long leadershipTermId,
        final long logPosition,
        final int leaderMemberId,
        final ChangeType changeType,
        final int memberId,
        final String clusterMembers)
    {
        if (leadershipTermId == this.leadershipTermId)
        {
            if (ChangeType.JOIN == changeType)
            {
                final ClusterMember[] newMembers = ClusterMember.parse(clusterMembers);
                if (memberId == this.memberId)
                {
                    activeMembers = newMembers;
                    clusterMemberByIdMap.clear();
                    clusterMemberByIdMap.compact();
                    ClusterMember.addClusterMemberIds(newMembers, clusterMemberByIdMap);
                    thisMember = ClusterMember.findMember(activeMembers, memberId);
                    leaderMember = ClusterMember.findMember(activeMembers, leaderMemberId);

                    ClusterMember.addConsensusPublications(
                        newMembers,
                        thisMember,
                        ctx.consensusChannel(),
                        ctx.consensusStreamId(),
                        aeron,
                        ctx.countedErrorHandler());
                }
                else
                {
                    clusterMemberJoined(memberId, newMembers);
                }
            }
            else if (ChangeType.QUIT == changeType)
            {
                if (memberId == this.memberId)
                {
                    state(ConsensusModule.State.QUITTING);
                }
                else
                {
                    clusterMemberQuit(memberId);
                    if (leaderMemberId == memberId && null == election)
                    {
                        commitPosition.proposeMaxOrdered(logPosition);
                        enterElection();
                    }
                }
            }

            if (null != election)
            {
                election.onMembershipChange(activeMembers, changeType, memberId, logPosition);
            }
        }
    }

    void onLoadSession(
        final long clusterSessionId,
        final long correlationId,
        final long openedPosition,
        final long timeOfLastActivity,
        final CloseReason closeReason,
        final int responseStreamId,
        final String responseChannel)
    {
        sessionByIdMap.put(clusterSessionId, new ClusterSession(
            clusterSessionId,
            correlationId,
            openedPosition,
            timeOfLastActivity,
            responseStreamId,
            createResponseChannel(responseChannel),
            closeReason));

        if (clusterSessionId >= nextSessionId)
        {
            nextSessionId = clusterSessionId + 1;
        }
    }

    void onLoadPendingMessage(final DirectBuffer buffer, final int offset, final int length)
    {
        pendingServiceMessages.append(buffer, offset, length);
    }

    void onLoadConsensusModuleState(
        final long nextSessionId,
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity)
    {
        this.nextSessionId = nextSessionId;
        this.nextServiceSessionId = nextServiceSessionId;
        this.logServiceSessionId = logServiceSessionId;
        pendingServiceMessages.reset(pendingMessageCapacity);
    }

    void onLoadClusterMembers(final int memberId, final int highMemberId, final String members)
    {
        if (null == dynamicJoin && !ctx.clusterMembersIgnoreSnapshot())
        {
            if (NULL_VALUE == this.memberId)
            {
                this.memberId = memberId;
                ctx.clusterMarkFile().memberId(memberId);
            }

            if (ClusterMember.EMPTY_MEMBERS == activeMembers)
            {
                activeMembers = ClusterMember.parse(members);
                this.highMemberId = Math.max(ClusterMember.highMemberId(activeMembers), highMemberId);
                rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
                thisMember = clusterMemberByIdMap.get(memberId);

                ClusterMember.addConsensusPublications(
                    activeMembers,
                    thisMember,
                    ctx.consensusChannel(),
                    ctx.consensusStreamId(),
                    aeron,
                    ctx.countedErrorHandler());
            }
        }
    }

    //Called from Election when in `LEADER_LOG_REPLICATION`, when in this state, the leader
    //is waiting for followers to replicate their missing logs.
    //return publication.sessionId
    int addLogPublication()
    {
        final long logPublicationTag = aeron.nextCorrelationId();
        logPublicationChannelTag = aeron.nextCorrelationId();

        // set up log publication
        final ChannelUri channelUri = ChannelUri.parse(ctx.logChannel());
        //add alias name
        channelUri.put(ALIAS_PARAM_NAME, "log");
        //add tags
        channelUri.put(TAGS_PARAM_NAME, logPublicationChannelTag + "," + logPublicationTag);

        //If use udp as log channel
        if (channelUri.isUdp())
        {
            //Use min as flow control strategy if no one specified.
            if (!channelUri.containsKey(FLOW_CONTROL_PARAM_NAME))
            {
                final long timeout = TimeUnit.NANOSECONDS.toSeconds(ctx.leaderHeartbeatTimeoutNs());
                channelUri.put(FLOW_CONTROL_PARAM_NAME, "min,t:" + timeout + "s");
            }

            //if use mdc, then set mdc control mode to: manual
            if (ctx.isLogMdc())
            {
                channelUri.put(MDC_CONTROL_MODE_PARAM_NAME, MDC_CONTROL_MODE_MANUAL);
            }

            //If there is only one member in the cluster, set ssc = true
            channelUri.put(SPIES_SIMULATE_CONNECTION_PARAM_NAME, Boolean.toString(activeMembers.length == 1));
        }

        if (null != recoveryPlan.log)
        {
            channelUri.initialPosition(
                recoveryPlan.appendedLogPosition, recoveryPlan.log.initialTermId, recoveryPlan.log.termBufferLength);
            channelUri.put(MTU_LENGTH_PARAM_NAME, Integer.toString(recoveryPlan.log.mtuLength));
        }
        else
        {
            ensureConsistentInitialTermId(channelUri);
        }


        final String channel = channelUri.toString();
        final ExclusivePublication publication = aeron.addExclusivePublication(channel, ctx.logStreamId());
        logPublisher.publication(publication);

        //add destination, this uses MDS.
        if (ctx.isLogMdc())
        {
            for (final ClusterMember member : activeMembers)
            {
                if (member.id() != memberId)
                {
                    logPublisher.addDestination(true, member.logEndpoint());
                }
            }

            for (final ClusterMember member : passiveMembers)
            {
                logPublisher.addDestination(true, member.logEndpoint());
            }
        }

        return publication.sessionId();
    }

    /**
     * Called back form Election.
     *The node joins the cluster as the leader.
     * @param logPosition the position the leader has consumed.
     */
    void joinLogAsLeader(
        final long leadershipTermId, final long logPosition, final int logSessionId, final boolean isStartup)
    {

        final boolean isIpc = ctx.logChannel().startsWith(IPC_CHANNEL);
        final String channel = (isIpc ? IPC_CHANNEL : UDP_CHANNEL) +
            "?tags=" + logPublicationChannelTag + "|session-id=" + logSessionId + "|alias=log";

        this.leadershipTermId = leadershipTermId;

        //begin recording raft log, with param session-id=logSessionId in the channel
        startLogRecording(channel, ctx.logStreamId(), SourceLocation.LOCAL);

        //create log append position in aeron counters with logSessionId.
        //this is where the `appendPosition` counter is created.
        while (!tryCreateAppendPosition(logSessionId))
        {
            idle();
        }

        //Tell service module to create a logAdapter with the logPosition for consuming raft logs.
        awaitServicesReady(
            isIpc ? channel : SPY_PREFIX + channel,
            ctx.logStreamId(),
            logSessionId,
            logPosition,
            Long.MAX_VALUE,
            isStartup,
            Cluster.Role.LEADER);
    }

    void liveLogDestination(final String liveLogDestination)
    {
        this.liveLogDestination = liveLogDestination;
    }

    String liveLogDestination()
    {
        return liveLogDestination;
    }

    void catchupLogDestination(final String catchupLogDestination)
    {
        this.catchupLogDestination = catchupLogDestination;
    }

    String catchupLogDestination()
    {
        return catchupLogDestination;
    }

    boolean tryJoinLogAsFollower(final Image image, final boolean isLeaderStartup, final long nowNs)
    {
        final Subscription logSubscription = image.subscription();

        if (NULL_VALUE == logSubscriptionId)
        {
            startLogRecording(logSubscription.channel(), logSubscription.streamId(), SourceLocation.REMOTE);
        }

        if (tryCreateAppendPosition(image.sessionId()))
        {
            appendDynamicJoinTermAndSnapshots();

            logAdapter.image(image);
            lastAppendPosition = image.joinPosition();
            timeOfLastAppendPositionUpdateNs = nowNs;

            awaitServicesReady(
                logSubscription.channel(),
                logSubscription.streamId(),
                image.sessionId(),
                image.joinPosition(),
                Long.MAX_VALUE,
                isLeaderStartup,
                Cluster.Role.FOLLOWER);

            return true;
        }

        return false;
    }

    void awaitServicesReady(
        final String logChannel,
        final int streamId,
        final int logSessionId,
        final long logPosition,
        final long maxLogPosition,
        final boolean isStartup,
        final Cluster.Role role)
    {
        //tell ClusteredServiceAgent to join log
        //so that ClusteredServiceAgent could create a logAdapter and poll raft logs with positions.
        serviceProxy.joinLog(
            logPosition,
            maxLogPosition,
            memberId,
            logSessionId,
            streamId,
            isStartup,
            role,
            logChannel);

        //wait for service module to ack this position
        expectedAckPosition = logPosition;
        //wait if all service module has acked this position.
        //Note: if there is more than one ClusterServices, then it will wait for all the services to ack this position.
        while (!ServiceAck.hasReached(logPosition, serviceAckId, serviceAckQueues))
        {
            //poll so that ServiceAck messages could be received and put into `serviceAckQueues`.
            idle(consensusModuleAdapter.poll());
        }

        //remove the first ServiceAck messages in every service.
        ServiceAck.removeHead(serviceAckQueues);

        //increase ackId for the next ack.
        ++serviceAckId;
    }

    LogReplay newLogReplay(final long logPosition, final long appendPosition)
    {
        return new LogReplay(
            archive,
            logRecordingId,
            logPosition,
            appendPosition,
            logAdapter,
            ctx);
    }

    int replayLogPoll(final LogAdapter logAdapter, final long stopPosition)
    {
        int workCount = 0;

        if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
        {
            final int fragments = logAdapter.poll(stopPosition);
            final long position = logAdapter.position();

            if (fragments > 0)
            {
                commitPosition.setOrdered(position);
            }
            else if (logAdapter.isImageClosed() && position < stopPosition)
            {
                throw new ClusterEvent("unexpected image close when replaying log: position=" + position);
            }

            workCount += fragments;
        }

        workCount += consensusModuleAdapter.poll();

        return workCount;
    }

    long logRecordingId()
    {
        return logRecordingId;
    }

    void logRecordingId(final long recordingId)
    {
        if (NULL_VALUE != recordingId)
        {
            logRecordingId = recordingId;
        }
    }

    void truncateLogEntry(final long leadershipTermId, final long logPosition)
    {
        archive.stopAllReplays(logRecordingId);
        archive.truncateRecording(logRecordingId, logPosition);
        if (NULL_VALUE != leadershipTermId)
        {
            recordingLog.commitLogPosition(leadershipTermId, logPosition);
        }
        logAdapter.disconnect(ctx.countedErrorHandler(), logPosition);
    }

    boolean appendNewLeadershipTermEvent(final long nowNs)
    {
        return logPublisher.appendNewLeadershipTermEvent(
            leadershipTermId,
            clusterClock.timeUnit().convert(nowNs, TimeUnit.NANOSECONDS),
            election.logPosition(),
            memberId,
            logPublisher.sessionId(),
            clusterTimeUnit,
            ctx.appVersion());
    }

    //Called back from Election.
    //when election is completed.
    void electionComplete(final long nowNs)
    {
        leadershipTermId = election.leadershipTermId();

        if (Cluster.Role.LEADER == role)
        {
            timeOfLastLogUpdateNs = nowNs - leaderHeartbeatIntervalNs;
            timerService.currentTime(clusterClock.timeUnit().convert(nowNs, TimeUnit.NANOSECONDS));
            ClusterControl.ToggleState.activate(controlToggle);
            prepareSessionsForNewTerm(election.isLeaderStartup());
        }
        else
        {
            timeOfLastLogUpdateNs = nowNs;
            timeOfLastAppendPositionUpdateNs = nowNs;
            timeOfLastAppendPositionSendNs = nowNs;
        }

        recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount(), logRecordingId);

        final long logPosition = election.logPosition();
        notifiedCommitPosition = logPosition;
        commitPosition.setOrdered(logPosition);
        pendingServiceMessages.consume(followerServiceSessionMessageSweeper, Integer.MAX_VALUE);
        updateMemberDetails(election.leader());
        election = null;

        connectIngress();
    }

    boolean dynamicJoinComplete(final long nowNs)
    {
        if (0 == activeMembers.length)
        {
            activeMembers = dynamicJoin.clusterMembers();
            ClusterMember.addClusterMemberIds(activeMembers, clusterMemberByIdMap);
            leaderMember = dynamicJoin.leader();

            ClusterMember.addConsensusPublications(
                activeMembers,
                thisMember,
                ctx.consensusChannel(),
                ctx.consensusStreamId(),
                aeron,
                ctx.countedErrorHandler());
        }

        if (NULL_VALUE == memberId)
        {
            memberId = dynamicJoin.memberId();
            ctx.clusterMarkFile().memberId(memberId);
            thisMember.id(memberId);
        }

        dynamicJoin = null;

        election = new Election(
            false,
            leadershipTermId,
            commitPosition.getWeak(),
            recoveryPlan.appendedLogPosition,
            activeMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            this);

        election.doWork(nowNs);

        return true;
    }

    void trackCatchupCompletion(
        final ClusterMember follower,
        final long leadershipTermId,
        final short appendPositionFlags)
    {
        if (NULL_VALUE != follower.catchupReplaySessionId() || isCatchupAppendPosition(appendPositionFlags))
        {
            if (follower.logPosition() >= logPublisher.position())
            {
                if (NULL_VALUE != follower.catchupReplayCorrelationId())
                {
                    if (archive.archiveProxy().stopReplay(
                        follower.catchupReplaySessionId(), aeron.nextCorrelationId(), archive.controlSessionId()))
                    {
                        follower.catchupReplayCorrelationId(NULL_VALUE);
                    }
                }

                if (consensusPublisher.stopCatchup(follower.publication(), leadershipTermId, follower.id()))
                {
                    follower.catchupReplaySessionId(NULL_VALUE);
                }
            }
        }
    }

    void catchupInitiated(final long nowNs)
    {
        timeOfLastAppendPositionUpdateNs = nowNs;
        timeOfLastAppendPositionSendNs = nowNs;
    }

    int catchupPoll(final long limitPosition, final long nowNs)
    {
        int workCount = 0;

        if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
        {
            final int fragments = logAdapter.poll(Math.min(appendPosition.get(), limitPosition));
            workCount += fragments;
            if (fragments == 0 && logAdapter.image().isClosed())
            {
                throw new ClusterEvent(
                    "unexpected image close during catchup: position=" + logAdapter.image().position());
            }

            final ExclusivePublication publication = election.leader().publication();
            workCount += updateFollowerPosition(
                publication, nowNs, leadershipTermId, appendPosition.get(), APPEND_POSITION_FLAG_CATCHUP);
            commitPosition.proposeMaxOrdered(logAdapter.position());
        }

        if (nowNs > (timeOfLastAppendPositionUpdateNs + leaderHeartbeatTimeoutNs) &&
            ConsensusModule.State.ACTIVE == state)
        {
            throw new ClusterEvent(
                "no catchup progress commitPosition=" + commitPosition.getWeak() + " limitPosition=" + limitPosition +
                    " lastAppendPosition=" + lastAppendPosition +
                    " appendPosition=" + (null != appendPosition ? appendPosition.get() : -1) +
                    " logPosition=" + election.logPosition());
        }

        workCount += consensusModuleAdapter.poll();

        return workCount;
    }

    boolean isCatchupNearLive(final long position)
    {
        final Image image = logAdapter.image();
        if (null != image)
        {
            final long localPosition = image.position();
            final long window = Math.min(image.termBufferLength() >> 2, LIVE_ADD_MAX_WINDOW);

            return localPosition >= (position - window);
        }

        return false;
    }

    void stopAllCatchups()
    {
        for (final ClusterMember member : activeMembers)
        {
            if (member.catchupReplaySessionId() != NULL_VALUE)
            {
                if (member.catchupReplayCorrelationId() != NULL_VALUE)
                {
                    try
                    {
                        archive.stopReplay(member.catchupReplaySessionId());
                    }
                    catch (final Exception ex)
                    {
                        ctx.countedErrorHandler().onError(new ClusterException("catchup already stopped", ex, WARN));
                    }
                }

                member.catchupReplaySessionId(NULL_VALUE);
                member.catchupReplayCorrelationId(NULL_VALUE);
            }
        }
    }

    void retrievedSnapshot(final long localRecordingId, final RecordingLog.Snapshot leaderSnapshot)
    {
        dynamicJoinSnapshots.add(new RecordingLog.Snapshot(
            localRecordingId,
            leaderSnapshot.leadershipTermId,
            leaderSnapshot.termBaseLogPosition,
            leaderSnapshot.logPosition,
            leaderSnapshot.timestamp,
            leaderSnapshot.serviceId));
    }

    Counter loadSnapshotsForDynamicJoin()
    {
        recoveryPlan = RecordingLog.createRecoveryPlan(dynamicJoinSnapshots);

        final Counter recoveryStateCounter = addRecoveryStateCounter(recoveryPlan);
        if (!recoveryPlan.snapshots.isEmpty())
        {
            loadSnapshot(recoveryPlan.snapshots.get(0), archive);
        }

        return recoveryStateCounter;
    }

    boolean pollForSnapshotLoadAck(final Counter recoveryStateCounter, final long nowNs)
    {
        consensusModuleAdapter.poll();

        if (ServiceAck.hasReached(expectedAckPosition, serviceAckId, serviceAckQueues))
        {
            captureServiceClientIds();
            ++serviceAckId;
            timeOfLastLogUpdateNs = nowNs;
            CloseHelper.close(ctx.countedErrorHandler(), recoveryStateCounter);
            state(ConsensusModule.State.ACTIVE);

            return true;
        }

        return false;
    }

    int pollArchiveEvents()
    {
        int workCount = 0;

        if (null != archive)
        {
            //poll archive events
            final RecordingSignalPoller poller = this.recordingSignalPoller;
            workCount += poller.poll();

            if (poller.isPollComplete())
            {
                final int templateId = poller.templateId();

                //if there is error in control response, handle errors
                if (ControlResponseDecoder.TEMPLATE_ID == templateId && poller.code() == ControlResponseCode.ERROR)
                {
                    for (final ClusterMember member : activeMembers)
                    {
                        if (member.catchupReplayCorrelationId() == poller.correlationId())
                        {
                            member.catchupReplaySessionId(NULL_VALUE);
                            member.catchupReplayCorrelationId(NULL_VALUE);

                            ctx.countedErrorHandler().onError(new ClusterEvent(
                                "catchup replay failed - " + poller.errorMessage()));
                            return workCount;
                        }
                    }

                    final ArchiveException ex = new ArchiveException(
                        poller.errorMessage(), (int)poller.relevantId(), poller.correlationId());

                    if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
                    {
                        ctx.countedErrorHandler().onError(ex);
                        unexpectedTermination();
                    }

                    if (null != election)
                    {
                        election.handleError(clusterClock.timeNanos(), ex);
                    }
                }
                else if (RecordingSignalEventDecoder.TEMPLATE_ID == templateId) //handle RecordingSignalEvent
                {
                    final long recordingId = poller.recordingId();
                    final long position = poller.recordingPosition();
                    final RecordingSignal signal = poller.recordingSignal();

                    //if log recording is stopped, update logRecordedPosition to recorded position.
                    if (RecordingSignal.STOP == signal && recordingId == logRecordingId)
                    {
                        this.logRecordedPosition = position;
                    }

                    //if there is an election in progress, let election handle onRecordingSignal.
                    if (null != election)
                    {
                        election.onRecordingSignal(poller.correlationId(), recordingId, position, signal);
                    }

                    //if there is dynamicJoin in progress, let dynamicJoin handle onRecordingSignal.
                    if (null != dynamicJoin)
                    {
                        dynamicJoin.onRecordingSignal(poller.correlationId(), recordingId, position, signal);
                    }
                }
            }
            else if (0 == workCount && !poller.subscription().isConnected())
            {
                ctx.countedErrorHandler().onError(new ClusterEvent("local archive is not connected"));
                unexpectedTermination();
            }
        }

        return workCount;
    }

    private void logNewLeadershipTerm(
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int memberId,
        final int leaderId,
        final int logSessionId,
        final boolean isStartup)
    {
    }

    private void logCommitPosition(
        final long leadershipTermId,
        final long logPosition,
        final int leaderMemberId,
        final int memberId)
    {
    }

    private void logAddPassiveMember(
        final long correlationId,
        final String memberEndpoints,
        final int memberId)
    {
    }

    private void logReplayNewLeadershipTermEvent(
        final int memberId,
        final boolean isInElection,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
    }

    //start recording raft log.
    private void startLogRecording(final String channel, final int streamId, final SourceLocation sourceLocation)
    {
        try
        {
            final long logRecordingId = recordingLog.findLastTermRecordingId();

            logSubscriptionId = RecordingPos.NULL_RECORDING_ID == logRecordingId ?
                archive.startRecording(channel, streamId, sourceLocation, true) :
                archive.extendRecording(logRecordingId, channel, streamId, sourceLocation, true);
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                ctx.countedErrorHandler().onError(ex);
                unexpectedTermination();
            }

            throw ex;
        }
    }

    private void prepareSessionsForNewTerm(final boolean isStartup)
    {
        if (isStartup)
        {
            for (final ClusterSession session : sessionByIdMap.values())
            {
                if (session.state() == OPEN)
                {
                    session.closing(CloseReason.TIMEOUT);
                }
            }
        }
        else
        {
            for (final ClusterSession session : sessionByIdMap.values())
            {
                if (session.state() == OPEN)
                {
                    session.connect(ctx.countedErrorHandler(), aeron);
                }
            }

            final long nowNs = clusterClock.timeNanos();
            for (final ClusterSession session : sessionByIdMap.values())
            {
                if (session.state() == OPEN)
                {
                    session.timeOfLastActivityNs(nowNs);
                    session.hasNewLeaderEventPending(true);
                }
            }
        }
    }

    private void updateMemberDetails(final ClusterMember newLeader)
    {
        leaderMember = newLeader;

        for (final ClusterMember clusterMember : activeMembers)
        {
            clusterMember.isLeader(clusterMember.id() == leaderMember.id());
        }

        ingressEndpoints = ClusterMember.ingressEndpoints(activeMembers);
    }

    /**
     * slow tick work
     * 1. invoke aeron ClientConductor.doWork.
     * 2. update ClusterMarkFile.activityTimestamp every 1 second.
     * 3. poll archive errors and recording signals and handle respectively.
     * 4. handle client session redirection and rejection.
     * 5. handle election tasks if election is in progress.
     *
     * @param nowNs current timestamp in nano seconds.
     * @return workCount
     */
    private int slowTickWork(final long nowNs)
    {
        // invoke aeron ClientConductor.doWork
        int workCount = aeronClientInvoker.invoke();

        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }
        else if (ConsensusModule.State.CLOSED == state)
        {
            unexpectedTermination();
        }
        else if (isElectionRequired)
        {
            //enter election stage when election is required.
            if (null == election)
            {
                enterElection();
            }
            isElectionRequired = false;
        }

        //update cluster markFile timestamp periodically
        if (nowNs >= markFileUpdateDeadlineNs)
        {
            markFileUpdateDeadlineNs = nowNs + MARK_FILE_UPDATE_INTERVAL_NS;
            markFile.updateActivityTimestamp(clusterClock.timeMillis());
        }

        //poll archive errors and recording signals and handle respectively.
        workCount += pollArchiveEvents();
        //redirect client to leaders
        workCount += sendRedirects(redirectSessions, nowNs);
        //send errors to clients
        workCount += sendRejections(rejectedSessions, nowNs);

        //If an election is in progress, handle election related tasks.
        if (null == election)
        {
            if (Cluster.Role.LEADER == role)
            {
                //Check ControlToggle
                workCount += checkControlToggle(nowNs);

                if (ConsensusModule.State.ACTIVE == state)
                {
                    //process pending sessions, to change CONNECT sessions to OPEN sessions.
                    workCount += processPendingSessions(pendingSessions, nowNs);
                    //check session liveliness, close if timeouts.
                    workCount += checkSessions(sessionByIdMap, nowNs);
                    //todo: passive members
                    workCount += processPassiveMembers(passiveMembers);

                    // Check if the cluster leader has an active quorum of cluster followers.
                    // If not, enter election stage.
                    if (!ClusterMember.hasActiveQuorum(activeMembers, nowNs, leaderHeartbeatTimeoutNs))
                    {
                        ctx.countedErrorHandler().onError(new ClusterEvent("inactive follower quorum"));
                        enterElection();
                        workCount += 1;
                    }
                }
                else if (ConsensusModule.State.TERMINATING == state)
                {
                    if (clusterTermination.canTerminate(activeMembers, terminationPosition, nowNs))
                    {
                        recordingLog.commitLogPosition(leadershipTermId, terminationPosition);
                        closeAndTerminate();
                    }
                }
            }
            else if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
            {
                if (nowNs >= (timeOfLastLogUpdateNs + leaderHeartbeatTimeoutNs) && NULL_POSITION == terminationPosition)
                {
                    ctx.countedErrorHandler().onError(new ClusterEvent("leader heartbeat timeout"));
                    enterElection();
                    workCount += 1;
                }
            }
        }

        return workCount;
    }

    /**
     * If current node is Leader:
     * 1. poll timer tasks and append it into log
     * 2. handle pending service messages
     * 3. poll client messages from IngressAdapter.
     * 4. update leader position if commitPosition has advanced.
     * 5. poll consensus messages from ConsensusModuleAdapter
     * @param timestamp now in milliseconds.
     * @param nowNs     now in nanoseconds.
     * @return
     */
    private int consensusWork(final long timestamp, final long nowNs)
    {
        int workCount = 0;

        if (Cluster.Role.LEADER == role)
        {
            if (ConsensusModule.State.ACTIVE == state)
            {
                //poll timer tasks and append to raft log
                workCount += timerService.poll(timestamp);
                //todo
                workCount += pendingServiceMessages.forEach(
                    pendingServiceMessageHeadOffset, serviceSessionMessageAppender, SERVICE_MESSAGE_LIMIT);

                //poll ingress messages from client
                workCount += ingressAdapter.poll();
            }

            //update leader commit position and tell every other member in the cluster.
            workCount += updateLeaderPosition(nowNs);
        }
        else
        {
            if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
            {
                if (NULL_POSITION != terminationPosition && logAdapter.position() >= terminationPosition)
                {
                    serviceProxy.terminationPosition(terminationPosition, ctx.countedErrorHandler());
                    state(ConsensusModule.State.TERMINATING);
                }
                else
                {
                    final long limit = null != appendPosition ? appendPosition.get() : logRecordedPosition;
                    final int count = logAdapter.poll(min(notifiedCommitPosition, limit));
                    if (0 == count && logAdapter.isImageClosed())
                    {
                        ctx.countedErrorHandler().onError(new ClusterEvent("log disconnected from leader"));
                        enterElection();
                        return 1;
                    }

                    commitPosition.proposeMaxOrdered(logAdapter.position());
                    workCount += ingressAdapter.poll();
                    workCount += count;
                }
            }

            //if current node is not leader, update follower commit position,
            // and only tell the leader its commit position.
            workCount += updateFollowerPosition(nowNs);
        }

        //to poll messages from service module.
        workCount += consensusModuleAdapter.poll();

        return workCount;
    }

    //Check ClusterControl by state.
    private int checkControlToggle(final long nowNs)
    {
        switch (ClusterControl.ToggleState.get(controlToggle))
        {
            case SUSPEND:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SUSPEND))
                {
                    state(ConsensusModule.State.SUSPENDED);
                }
                break;

            case RESUME:
                if (ConsensusModule.State.SUSPENDED == state && appendAction(ClusterAction.RESUME))
                {
                    state(ConsensusModule.State.ACTIVE);
                    ClusterControl.ToggleState.reset(controlToggle);
                }
                break;

            case SNAPSHOT:
                //1. append snapshot action to raft log
                //2. set consensus module state to SNAPSHOT
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SNAPSHOT))
                {
                    state(ConsensusModule.State.SNAPSHOT);
                }
                break;

            case SHUTDOWN:
                if (ConsensusModule.State.ACTIVE == state && appendAction(ClusterAction.SNAPSHOT))
                {
                    final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
                    final long position = logPublisher.position();
                    clusterTermination = new ClusterTermination(nowNs + ctx.terminationTimeoutNs());
                    clusterTermination.terminationPosition(
                        errorHandler, consensusPublisher, activeMembers, thisMember, leadershipTermId, position);
                    terminationPosition = position;
                    state(ConsensusModule.State.SNAPSHOT);
                }
                break;

            case ABORT:
                if (ConsensusModule.State.ACTIVE == state)
                {
                    final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
                    final long position = logPublisher.position();
                    clusterTermination = new ClusterTermination(nowNs + ctx.terminationTimeoutNs());
                    clusterTermination.terminationPosition(
                        errorHandler, consensusPublisher, activeMembers, thisMember, leadershipTermId, position);
                    terminationPosition = position;
                    serviceProxy.terminationPosition(terminationPosition, errorHandler);
                    state(ConsensusModule.State.TERMINATING);
                }
                break;

            default:
                return 0;
        }

        return 1;
    }

    private boolean appendAction(final ClusterAction action)
    {
        return logPublisher.appendClusterAction(leadershipTermId, clusterClock.time(), action);
    }

    //handle pendingSessions,
    //pendingSessions
    private int processPendingSessions(final ArrayList<ClusterSession> pendingSessions, final long nowNs)
    {
        int workCount = 0;

        //process from the last one to the first one.
        for (int lastIndex = pendingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = pendingSessions.get(i);

            if (session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler());
                continue;
            }

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs) && session.state() != INIT)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler());
                ctx.timedOutClientCounter().incrementOrdered();
                continue;
            }

            if (session.state() == INIT || session.state() == CONNECTING || session.state() == CONNECTED)
            {
                if (session.isResponsePublicationConnected(aeron, nowNs))
                {
                    session.state(CONNECTED);
                    authenticator.onConnectedSession(sessionProxy.session(session), clusterClock.timeMillis());
                }
            }

            if (session.state() == CHALLENGED)
            {
                if (session.isResponsePublicationConnected(aeron, nowNs))
                {
                    authenticator.onChallengedSession(sessionProxy.session(session), clusterClock.timeMillis());
                }
            }

            if (session.state() == AUTHENTICATED)
            {
                if (session.isBackupSession())
                {
                    final RecordingLog.Entry entry = recordingLog.findLastTerm();
                    if (null != entry && consensusPublisher.backupResponse(
                        session,
                        commitPosition.id(),
                        leaderMember.id(),
                        entry,
                        recoveryPlan,
                        ClusterMember.encodeAsString(activeMembers)))
                    {
                        ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                        session.close(aeron, ctx.countedErrorHandler());
                        workCount += 1;
                    }
                }
                //If a session is open, append SessionOpen event to raft log,
                // and remove it from pendingSessions, and add it into sessionByIdMap.
                //If append SessionOpen failed, it will be rechecked later.
                else if (appendSessionAndOpen(session, nowNs))
                {
                    ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                    sessionByIdMap.put(session.id(), session);
                    workCount += 1;
                }
            }
            else if (session.state() == REJECTED)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSessions, i, lastIndex--);
                rejectedSessions.add(session);
            }
        }

        return workCount;
    }

    /**
     * Send errors to clients.
     * Remove ClientSession from redirectSessions if any of the conditions match:
     * 1) Errors has been sent successfully
     * 2) ClientSession has been timed out.
     * 3) ClientSession is not in a valid state.
     * @param rejectedSessions
     * @param nowNs
     * @return
     */
    private int sendRejections(final ArrayList<ClusterSession> rejectedSessions, final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = rejectedSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = rejectedSessions.get(i);
            final String detail = session.responseDetail();
            final EventCode eventCode = session.eventCode();

            if ((session.isResponsePublicationConnected(aeron, nowNs) &&
                egressPublisher.sendEvent(session, leadershipTermId, leaderMember.id(), eventCode, detail)) ||
                (session.state() != INIT && nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs)) ||
                session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(rejectedSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler());
                workCount++;
            }
        }

        return workCount;
    }

    /**
     * Send redirect to clientSessions to tell them to reconnect to leader node.
     * Remove ClientSession from redirectSessions if any of the conditions match:
     * 1) Redirect has been sent successfully
     * 2) ClientSession has been timed out.
     * 3) ClientSession is not in a valid state.
     *
     * @param redirectSessions
     * @param nowNs
     * @return
     */
    private int sendRedirects(final ArrayList<ClusterSession> redirectSessions, final long nowNs)
    {
        int workCount = 0;

        for (int lastIndex = redirectSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ClusterSession session = redirectSessions.get(i);
            final EventCode eventCode = EventCode.REDIRECT;
            final int leaderId = leaderMember.id();

            if ((session.isResponsePublicationConnected(aeron, nowNs) &&
                egressPublisher.sendEvent(session, leadershipTermId, leaderId, eventCode, ingressEndpoints)) ||
                (session.state() != INIT && nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs)) ||
                session.state() == INVALID)
            {
                ArrayListUtil.fastUnorderedRemove(redirectSessions, i, lastIndex--);
                session.close(aeron, ctx.countedErrorHandler());
                workCount++;
            }
        }

        return workCount;
    }

    private int processPassiveMembers(final ClusterMember[] passiveMembers)
    {
        int workCount = 0;

        for (final ClusterMember member : passiveMembers)
        {
            if (member.correlationId() != NULL_VALUE)
            {
                if (consensusPublisher.clusterMemberChange(
                    member.publication(),
                    member.correlationId(),
                    leaderMember.id(),
                    ClusterMember.encodeAsString(activeMembers),
                    ClusterMember.encodeAsString(passiveMembers)))
                {
                    member.correlationId(NULL_VALUE);
                    workCount++;
                }
            }
            else if (member.hasRequestedJoin() && member.logPosition() == logPublisher.position())
            {
                final ClusterMember[] newMembers = ClusterMember.addMember(activeMembers, member);
                final long timestamp = clusterClock.time();

                if (logPublisher.appendMembershipChangeEvent(
                    leadershipTermId,
                    timestamp,
                    leaderMember.id(),
                    newMembers.length,
                    ChangeType.JOIN,
                    member.id(),
                    ClusterMember.encodeAsString(newMembers)) > 0)
                {
                    timeOfLastLogUpdateNs = clusterTimeUnit.toNanos(timestamp) - leaderHeartbeatIntervalNs;
                    this.passiveMembers = ClusterMember.removeMember(this.passiveMembers, member.id());
                    activeMembers = newMembers;
                    rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
                    member.hasRequestedJoin(false);

                    workCount++;
                    break;
                }
            }
        }

        return workCount;
    }

    //check all OPEN sessions
    //1) close a open session if it timeouts.
    //2) handle CLOSING sessions.
    //Note: If a session timeouts, its state then becomes CLOSING, if appendSessionClose successfully,
    //then its state becomes CLOSED, it appendSessionClose failed,
    // a CLOSING state session will be rechecked until  appendSessionClose succeeds.
    private int checkSessions(final Long2ObjectHashMap<ClusterSession> sessionByIdMap, final long nowNs)
    {
        int workCount = 0;

        for (final Iterator<ClusterSession> i = sessionByIdMap.values().iterator(); i.hasNext(); )
        {
            final ClusterSession session = i.next();

            if (nowNs > (session.timeOfLastActivityNs() + sessionTimeoutNs))
            {
                if (session.state() == OPEN)
                {
                    session.closing(CloseReason.TIMEOUT);

                    //appendSessionClose to tell service module and other nodes.
                    if (logPublisher.appendSessionClose(
                        memberId, session, leadershipTermId, clusterClock.time(), clusterClock.timeUnit()))
                    {
                        final String msg = session.closeReason().name();
                        egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                        session.closedLogPosition(logPublisher.position());
                        uncommittedClosedSessions.addLast(session);
                        i.remove();
                        session.close(aeron, ctx.countedErrorHandler());
                        ctx.timedOutClientCounter().incrementOrdered();
                        workCount++;
                    }
                }
                else if (session.state() == CLOSING)
                {
                    if (logPublisher.appendSessionClose(
                        memberId, session, leadershipTermId, clusterClock.time(), clusterClock.timeUnit()))
                    {
                        final String msg = session.closeReason().name();
                        egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, msg);
                        session.closedLogPosition(logPublisher.position());
                        uncommittedClosedSessions.addLast(session);
                        i.remove();
                        session.close(aeron, ctx.countedErrorHandler());
                        if (session.closeReason() == CloseReason.TIMEOUT)
                        {
                            ctx.timedOutClientCounter().incrementOrdered();
                        }
                        workCount++;
                    }
                }
                else
                {
                    i.remove();
                    session.close(aeron, ctx.countedErrorHandler());
                    workCount++;
                }
            }
            else if (session.hasOpenEventPending())
            {
                workCount += sendSessionOpenEvent(session);
            }
            else if (session.hasNewLeaderEventPending())
            {
                workCount += sendNewLeaderEvent(session);
            }
        }

        return workCount;
    }

    private void captureServiceAck(final long logPosition, final long ackId, final long relevantId, final int serviceId)
    {
        if (0 == ackId && NULL_VALUE != serviceClientIds[serviceId])
        {
            throw new ClusterException(
                "initial ack already received from service: possible duplicate serviceId=" + serviceId);
        }

        serviceAckQueues[serviceId].offerLast(new ServiceAck(ackId, logPosition, relevantId));
    }

    private ServiceAck[] pollServiceAcks(final long logPosition, final int serviceId)
    {
        final ServiceAck[] serviceAcks = new ServiceAck[serviceAckQueues.length];
        for (int id = 0, length = serviceAckQueues.length; id < length; id++)
        {
            final ServiceAck serviceAck = serviceAckQueues[id].pollFirst();
            if (null == serviceAck || serviceAck.logPosition() != logPosition)
            {
                throw new ClusterException(
                    "invalid ack for serviceId=" + serviceId + " logPosition=" + logPosition + " " + serviceAck);
            }

            serviceAcks[id] = serviceAck;
        }

        return serviceAcks;
    }

    private int sendNewLeaderEvent(final ClusterSession session)
    {
        if (egressPublisher.newLeader(session, leadershipTermId, leaderMember.id(), ingressEndpoints))
        {
            session.hasNewLeaderEventPending(false);
            return 1;
        }

        return 0;
    }

    private int sendSessionOpenEvent(final ClusterSession session)
    {
        if (egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.OK, ""))
        {
            session.clearOpenEventPending();
            return 1;
        }

        return 0;
    }

    private boolean appendSessionAndOpen(final ClusterSession session, final long nowNs)
    {
        final long resultingPosition = logPublisher.appendSessionOpen(session, leadershipTermId, clusterClock.time());
        if (resultingPosition > 0)
        {
            session.open(resultingPosition);
            session.timeOfLastActivityNs(nowNs);
            return true;
        }

        return false;
    }

    private boolean tryCreateAppendPosition(final int logSessionId)
    {
        final CountersReader counters = aeron.countersReader();
        final int counterId = RecordingPos.findCounterIdBySession(counters, logSessionId);
        if (CountersReader.NULL_COUNTER_ID == counterId)
        {
            return false;
        }

        final long registrationId = counters.getCounterRegistrationId(counterId);
        if (0 == registrationId)
        {
            return false;
        }

        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        if (RecordingPos.NULL_RECORDING_ID == recordingId)
        {
            return false;
        }

        logRecordingId = recordingId;
        appendPosition = new ReadableCounter(counters, registrationId, counterId);
        logRecordedPosition = NULL_POSITION;

        return true;
    }

    //determine the commit position if current node is follower
    //and tell the leader the updated commitPosition.
    private int updateFollowerPosition(final long nowNs)
    {
        final long recordedPosition = null != appendPosition ? appendPosition.get() : logRecordedPosition;
        return updateFollowerPosition(
            leaderMember.publication(), nowNs, this.leadershipTermId, recordedPosition, APPEND_POSITION_FLAG_NONE);
    }

    private int updateFollowerPosition(
        final ExclusivePublication publication,
        final long nowNs,
        final long leadershipTermId,
        final long appendPosition,
        final short flags)
    {
        final long position = Math.max(appendPosition, lastAppendPosition);

        //update position only when
        //1) position has advanced since last time.
        //2) a fixed time window has passed.
        if ((position > lastAppendPosition ||
            nowNs >= (timeOfLastAppendPositionSendNs + leaderHeartbeatIntervalNs)))
        {
            //appendPosition may fail, it will retry next time if failed.
            //Note: inside appendPosition, it will also retry 3 times before it return false.
            if (consensusPublisher.appendPosition(publication, leadershipTermId, position, memberId, flags))
            {
                if (position > lastAppendPosition)
                {
                    lastAppendPosition = position;
                    timeOfLastAppendPositionUpdateNs = nowNs;
                }
                timeOfLastAppendPositionSendNs = nowNs;

                return 1;
            }
        }

        return 0;
    }

    private void loadSnapshot(final RecordingLog.Snapshot snapshot, final AeronArchive archive)
    {
        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final int sessionId = (int)archive.startReplay(snapshot.recordingId, 0, NULL_LENGTH, channel, streamId);
        final String replaySubscriptionChannel = ChannelUri.addSessionId(channel, sessionId);

        try (Subscription subscription = aeron.addSubscription(replaySubscriptionChannel, streamId))
        {
            final Image image = awaitImage(sessionId, subscription);
            final ConsensusModuleSnapshotLoader snapshotLoader = new ConsensusModuleSnapshotLoader(image, this);

            while (true)
            {
                final int fragments = snapshotLoader.poll();
                if (0 == fragments)
                {
                    if (snapshotLoader.isDone())
                    {
                        break;
                    }

                    if (image.isClosed())
                    {
                        pollArchiveEvents();
                        throw new ClusterException("snapshot ended unexpectedly: " + image);
                    }
                }

                idle(fragments);
            }

            final int appVersion = snapshotLoader.appVersion();
            if (SemanticVersion.major(ctx.appVersion()) != SemanticVersion.major(appVersion))
            {
                throw new ClusterException(
                    "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                    " snapshot=" + SemanticVersion.toString(appVersion));
            }

            final TimeUnit timeUnit = snapshotLoader.timeUnit();
            if (timeUnit != clusterTimeUnit)
            {
                throw new ClusterException("incompatible time unit: " + clusterTimeUnit + " snapshot=" + timeUnit);
            }

            pendingServiceMessages.forEach(ConsensusModuleAgent::serviceSessionMessageReset, Integer.MAX_VALUE);
        }

        timerService.currentTime(clusterClock.time());
        commitPosition.setOrdered(snapshot.logPosition);
        leadershipTermId = snapshot.leadershipTermId;
        expectedAckPosition = snapshot.logPosition;
    }

    private Image awaitImage(final int sessionId, final Subscription subscription)
    {
        idleStrategy.reset();
        Image image;
        while ((image = subscription.imageBySessionId(sessionId)) == null)
        {
            idle();
        }

        return image;
    }

    private Counter addRecoveryStateCounter(final RecordingLog.RecoveryPlan plan)
    {
        final int snapshotsCount = plan.snapshots.size();

        if (snapshotsCount > 0)
        {
            final long[] serviceSnapshotRecordingIds = new long[snapshotsCount - 1];
            final RecordingLog.Snapshot snapshot = plan.snapshots.get(0);

            for (int i = 1; i < snapshotsCount; i++)
            {
                final RecordingLog.Snapshot serviceSnapshot = plan.snapshots.get(i);
                serviceSnapshotRecordingIds[serviceSnapshot.serviceId] = serviceSnapshot.recordingId;
            }

            return RecoveryState.allocate(
                aeron,
                snapshot.leadershipTermId,
                snapshot.logPosition,
                snapshot.timestamp,
                ctx.clusterId(),
                serviceSnapshotRecordingIds);
        }

        return RecoveryState.allocate(aeron, leadershipTermId, 0, 0, ctx.clusterId());
    }

    private DynamicJoin requiresDynamicJoin()
    {
        if (0 == activeMembers.length && null != ctx.clusterConsensusEndpoints())
        {
            return new DynamicJoin(ctx.clusterConsensusEndpoints(), archive, consensusPublisher, ctx, this);
        }

        return null;
    }

    private void captureServiceClientIds()
    {
        for (int i = 0, length = serviceClientIds.length; i < length; i++)
        {
            final ServiceAck serviceAck = serviceAckQueues[i].pollFirst();
            serviceClientIds[i] = Objects.requireNonNull(serviceAck).relevantId();
        }
    }

    //Remove activeMembers
    private void handleMemberRemovals(final long commitPosition)
    {
        ClusterMember[] members = activeMembers;

        for (final ClusterMember member : activeMembers)
        {
            //If a member has removalPosition set, and its removalPosition is less than current commitPosition,
            //then it will be removed.
            //Which means sometimes a removal action may fail because of the removePosition.
            if (member.hasRequestedRemove() && member.removalPosition() <= commitPosition)
            {
                if (member.id() == memberId)
                {
                    state(ConsensusModule.State.QUITTING);
                }

                members = ClusterMember.removeMember(members, member.id());
                clusterMemberByIdMap.remove(member.id());
                clusterMemberByIdMap.compact();

                member.closePublication(ctx.countedErrorHandler());

                logPublisher.removeDestination(ctx.isLogMdc(), member.logEndpoint());
                pendingMemberRemovals--;
            }
        }

        activeMembers = members;
        rankedPositions = new long[ClusterMember.quorumThreshold(members.length)];
    }

    /**
     * determine leader position with appendPosition and qurom position from cluster members
     * and then tell every other members in the cluster the new commit position.
     * @param nowNs
     * @return
     */
    private int updateLeaderPosition(final long nowNs)
    {
        if (null != appendPosition)
        {
            return updateLeaderPosition(nowNs, appendPosition.get());
        }

        return 0;
    }

    long quorumPosition()
    {
        return ClusterMember.quorumPosition(activeMembers, rankedPositions);
    }

    int updateLeaderPosition(final long nowNs, final long position)
    {
        //update position and timestamp for current member.
        thisMember.logPosition(position).timeOfLastAppendPositionNs(nowNs);

        //determine commit position
        final long commitPosition = min(quorumPosition(), position);

        //only update if:
        //1) commitPosition has advanced.
        //2) a fixed time window has passed.
        if (commitPosition > this.commitPosition.getWeak() ||
            nowNs >= (timeOfLastLogUpdateNs + leaderHeartbeatIntervalNs))
        {
            //tell every other members the position.
            //Note: it does not care about if commitPosition was sent successfully to followers or not.
            //The follower has to make sure it sends its commitPosition successfully, if not, it will retry.
            //See: updateFollowerPosition.
            publishCommitPosition(commitPosition);

            //update commitPosition in the aeron counter
            this.commitPosition.setOrdered(commitPosition);
            timeOfLastLogUpdateNs = nowNs;

            //todo:
            clearUncommittedEntriesTo(commitPosition);

            //Remove members if there is pending members to be removed.
            //member removal is only invoked from ClusterTool.removeMember for now.
            if (pendingMemberRemovals > 0)
            {
                handleMemberRemovals(commitPosition);
            }

            return 1;
        }

        return 0;
    }

    //Tell the commitPosition to every other members in the clusters with consensusPublisher.
    //Note: It will not care about if commitPosition has been successfully sent or not.
    void publishCommitPosition(final long commitPosition)
    {
        for (final ClusterMember member : activeMembers)
        {
            if (member.id() != memberId)
            {
                consensusPublisher.commitPosition(member.publication(), leadershipTermId, commitPosition, memberId);
            }
        }
    }

    LogReplication newLogReplication(
        final String leaderArchiveEndpoint, final long leaderRecordingId, final long stopPosition, final long nowNs)
    {
        return new LogReplication(
            archive,
            leaderRecordingId,
            logRecordingId,
            stopPosition,
            ChannelUri.createDestinationUri(ctx.leaderArchiveControlChannel(), leaderArchiveEndpoint),
            ctx.replicationChannel(),
            ctx.leaderHeartbeatTimeoutNs(),
            ctx.leaderHeartbeatIntervalNs(),
            nowNs);
    }

    private void clearSessionsAfter(final long logPosition)
    {
        for (final Iterator<ClusterSession> i = sessionByIdMap.values().iterator(); i.hasNext(); )
        {
            final ClusterSession session = i.next();
            if (session.openedLogPosition() > logPosition)
            {
                i.remove();
                egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, "election");
                session.close(aeron, ctx.countedErrorHandler());
            }
        }

        for (final ClusterSession session : pendingSessions)
        {
            egressPublisher.sendEvent(session, leadershipTermId, memberId, EventCode.CLOSED, "election");
            session.close(aeron, ctx.countedErrorHandler());
        }

        pendingSessions.clear();
    }

    private void clearUncommittedEntriesTo(final long commitPosition)
    {
        if (uncommittedServiceMessages > 0)
        {
            pendingServiceMessageHeadOffset -= pendingServiceMessages.consume(
                leaderServiceSessionMessageSweeper, Integer.MAX_VALUE);
        }

        while (uncommittedTimers.peekLong() <= commitPosition)
        {
            uncommittedTimers.pollLong();
            uncommittedTimers.pollLong();
        }

        while (true)
        {
            final ClusterSession clusterSession = uncommittedClosedSessions.peekFirst();
            if (null == clusterSession || clusterSession.closedLogPosition() > commitPosition)
            {
                break;
            }

            uncommittedClosedSessions.pollFirst();
        }
    }

    private void restoreUncommittedEntries(final long commitPosition)
    {
        for (final LongArrayQueue.LongIterator i = uncommittedTimers.iterator(); i.hasNext(); )
        {
            final long appendPosition = i.nextValue();
            final long correlationId = i.nextValue();

            if (appendPosition > commitPosition)
            {
                timerService.scheduleTimerForCorrelationId(correlationId, 0);
            }
        }
        uncommittedTimers.clear();

        pendingServiceMessages.consume(followerServiceSessionMessageSweeper, Integer.MAX_VALUE);
        pendingServiceMessageHeadOffset = 0;

        if (uncommittedServiceMessages > 0)
        {
            pendingServiceMessages.consume(leaderServiceSessionMessageSweeper, Integer.MAX_VALUE);
            pendingServiceMessages.forEach(ConsensusModuleAgent::serviceSessionMessageReset, Integer.MAX_VALUE);
            uncommittedServiceMessages = 0;
        }

        ClusterSession session;
        while (null != (session = uncommittedClosedSessions.pollFirst()))
        {
            if (session.closedLogPosition() > commitPosition)
            {
                session.closedLogPosition(NULL_POSITION);
                session.state(CLOSING);
                sessionByIdMap.put(session.id(), session);
            }
        }
    }

    /**
     * Enter an election stage.
     * 1. change current node into Follower.
     * 2. create Election
     * 3. invoke Election.doWork
     */
    private void enterElection()
    {
        if (null != election)
        {
            throw new IllegalStateException("election in progress");
        }

        //Make current node a follower
        role(Cluster.Role.FOLLOWER);

        election = new Election(
            false,
            leadershipTermId,
            commitPosition.getWeak(),
            null != appendPosition ? appendPosition.get() : recoveryPlan.appendedLogPosition,
            activeMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            this);

        election.doWork(clusterClock.timeNanos());
    }

    private void idle()
    {
        checkInterruptStatus();
        aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        idleStrategy.idle();
        pollArchiveEvents();
    }

    private void idle(final int workCount)
    {
        checkInterruptStatus();
        aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        idleStrategy.idle(workCount);

        if (0 == workCount)
        {
            pollArchiveEvents();
        }
    }

    private static void checkInterruptStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("interrupted");
        }
    }

    //take raft log snapshot
    //after service snapshot has been taken and service module has sent back serviceAck.
    private void takeSnapshot(final long timestamp, final long logPosition, final ServiceAck[] serviceAcks)
    {
        final long recordingId;
        try (ExclusivePublication publication = aeron.addExclusivePublication(
            ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            //start recording the publication
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL, true);

            //find the recording position counter for the recording.
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounter(counters, publication.sessionId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            snapshotState(publication, logPosition, leadershipTermId);

            //wait for the recording position to reach the publication position
            awaitRecordingComplete(recordingId, publication.position(), counters, counterId);
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                ctx.countedErrorHandler().onError(ex);
                unexpectedTermination();
            }

            throw ex;
        }

        final long termBaseLogPosition = recordingLog.getTermEntry(leadershipTermId).termBaseLogPosition;

        for (int serviceId = serviceAcks.length - 1; serviceId >= 0; serviceId--)
        {
            final long snapshotId = serviceAcks[serviceId].relevantId();
            recordingLog.appendSnapshot(
                snapshotId, leadershipTermId, termBaseLogPosition, logPosition, timestamp, serviceId);
        }

        recordingLog.appendSnapshot(
            recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp, SERVICE_ID);

        recordingLog.force(ctx.fileSyncLevel());
        recoveryPlan = recordingLog.createRecoveryPlan(archive, ctx.serviceCount(), Aeron.NULL_VALUE);
        ctx.snapshotCounter().incrementOrdered();

        final long nowNs = clusterClock.timeNanos();
        for (final ClusterSession session : sessionByIdMap.values())
        {
            session.timeOfLastActivityNs(nowNs);
        }
    }

    private void awaitRecordingComplete(
        final long recordingId, final long position, final CountersReader counters, final int counterId)
    {
        idleStrategy.reset();
        while (counters.getCounterValue(counterId) < position)
        {
            idle();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
            }
        }
    }

    private int awaitRecordingCounter(final CountersReader counters, final int sessionId)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (CountersReader.NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        }

        return counterId;
    }

    private void snapshotState(
        final ExclusivePublication publication, final long logPosition, final long leadershipTermId)
    {
        final ConsensusModuleSnapshotTaker snapshotTaker = new ConsensusModuleSnapshotTaker(
            publication, idleStrategy, aeronClientInvoker);

        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, clusterTimeUnit, ctx.appVersion());

        snapshotTaker.snapshotConsensusModuleState(
            nextSessionId, nextServiceSessionId, logServiceSessionId, pendingServiceMessages.size());
        snapshotTaker.snapshotClusterMembers(memberId, highMemberId, ClusterMember.encodeAsString(activeMembers));

        for (final ClusterSession session : sessionByIdMap.values())
        {
            if (session.state() == OPEN || session.state() == CLOSED)
            {
                snapshotTaker.snapshotSession(session);
            }
        }

        timerService.snapshot(snapshotTaker);
        snapshotTaker.snapshot(pendingServiceMessages);

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, clusterTimeUnit, ctx.appVersion());
    }

    private void clusterMemberJoined(final int memberId, final ClusterMember[] newMembers)
    {
        highMemberId = Math.max(highMemberId, memberId);

        final ClusterMember eventMember = ClusterMember.findMember(newMembers, memberId);
        if (null != eventMember)
        {
            if (null == eventMember.publication())
            {
                ClusterMember.addConsensusPublication(
                    eventMember, ctx.consensusChannel(), ctx.consensusStreamId(), aeron, ctx.countedErrorHandler());
            }

            activeMembers = ClusterMember.addMember(activeMembers, eventMember);
            clusterMemberByIdMap.put(memberId, eventMember);
            rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
        }
    }

    private void clusterMemberQuit(final int memberId)
    {
        activeMembers = ClusterMember.removeMember(activeMembers, memberId);
        clusterMemberByIdMap.remove(memberId);
        rankedPositions = new long[ClusterMember.quorumThreshold(activeMembers.length)];
    }

    private void onUnavailableIngressImage(final Image image)
    {
        ingressAdapter.freeSessionBuffer(image.sessionId());
    }

    private void enqueueServiceSessionMessage(
        final MutableDirectBuffer buffer, final int offset, final int length, final long clusterSessionId)
    {
        final int headerOffset = offset - SessionMessageHeaderDecoder.BLOCK_LENGTH;
        final int clusterSessionIdOffset = headerOffset + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();
        final int timestampOffset = headerOffset + SessionMessageHeaderDecoder.timestampEncodingOffset();

        buffer.putLong(clusterSessionIdOffset, clusterSessionId, SessionMessageHeaderDecoder.BYTE_ORDER);
        buffer.putLong(timestampOffset, Long.MAX_VALUE, SessionMessageHeaderDecoder.BYTE_ORDER);
        if (!pendingServiceMessages.append(buffer, offset - SESSION_HEADER_LENGTH, length + SESSION_HEADER_LENGTH))
        {
            throw new ClusterException("pending service message buffer capacity: " + pendingServiceMessages.size());
        }
    }

    private boolean serviceSessionMessageAppender(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int headerOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int clusterSessionIdOffset = headerOffset + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();
        final int timestampOffset = headerOffset + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long clusterSessionId = buffer.getLong(clusterSessionIdOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        final long appendPosition = logPublisher.appendMessage(
            leadershipTermId,
            clusterSessionId,
            clusterClock.time(),
            buffer,
            offset + SESSION_HEADER_LENGTH,
            length - SESSION_HEADER_LENGTH);

        if (appendPosition > 0)
        {
            ++uncommittedServiceMessages;
            logServiceSessionId = clusterSessionId;
            pendingServiceMessageHeadOffset = headOffset;
            buffer.putLong(timestampOffset, appendPosition, SessionMessageHeaderEncoder.BYTE_ORDER);

            return true;
        }

        return false;
    }

    private static boolean serviceSessionMessageReset(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int timestampOffset = offset +
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long appendPosition = buffer.getLong(timestampOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        if (appendPosition < Long.MAX_VALUE)
        {
            buffer.putLong(timestampOffset, Long.MAX_VALUE, SessionMessageHeaderEncoder.BYTE_ORDER);
            return true;
        }

        return false;
    }

    private boolean leaderServiceSessionMessageSweeper(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int timestampOffset = offset +
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.timestampEncodingOffset();
        final long appendPosition = buffer.getLong(timestampOffset, SessionMessageHeaderDecoder.BYTE_ORDER);

        if (appendPosition <= commitPosition.getWeak())
        {
            --uncommittedServiceMessages;
            return true;
        }

        return false;
    }

    private boolean followerServiceSessionMessageSweeper(
        final MutableDirectBuffer buffer, final int offset, final int length, final int headOffset)
    {
        final int clusterSessionIdOffset = offset +
            MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.clusterSessionIdEncodingOffset();

        return buffer.getLong(clusterSessionIdOffset, SessionMessageHeaderDecoder.BYTE_ORDER) <= logServiceSessionId;
    }

    private void onUnavailableCounter(final CountersReader counters, final long registrationId, final int counterId)
    {
        if (ConsensusModule.State.TERMINATING != state && ConsensusModule.State.QUITTING != state)
        {
            for (final long clientId : serviceClientIds)
            {
                if (registrationId == clientId)
                {
                    ctx.countedErrorHandler().onError(new ClusterEvent("Aeron client in service closed unexpectedly"));
                    state(ConsensusModule.State.CLOSED);
                    return;
                }
            }

            if (null != appendPosition && appendPosition.registrationId() == registrationId)
            {
                appendPosition = null;
                logSubscriptionId = NULL_VALUE;

                if (null != election)
                {
                    election.handleError(clusterClock.timeNanos(), new ClusterEvent(
                        "log recording ended unexpectedly (null != election)"));
                }
                else if (NULL_POSITION == terminationPosition)
                {
                    ctx.countedErrorHandler().onError(new ClusterEvent(
                        "log recording ended unexpectedly (NULL_POSITION == terminationPosition)"));
                    isElectionRequired = true;
                }
            }
        }
    }

    private void closeAndTerminate()
    {
        tryStopLogRecording();
        state(ConsensusModule.State.CLOSED);
        throw new ClusterTerminationException(true);
    }

    private void unexpectedTermination()
    {
        aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);
        serviceProxy.terminationPosition(0, ctx.countedErrorHandler());
        tryStopLogRecording();
        state(ConsensusModule.State.CLOSED);
        throw new ClusterTerminationException(false);
    }

    private void tryStopLogRecording()
    {
        appendPosition = null;

        if (NULL_VALUE != logSubscriptionId && archive.archiveProxy().publication().isConnected())
        {
            try
            {
                archive.tryStopRecording(logSubscriptionId);
            }
            catch (final Exception ex)
            {
                ctx.countedErrorHandler().onError(new ClusterException(ex, WARN));
            }

            logSubscriptionId = NULL_VALUE;
        }

        if (NULL_VALUE != logRecordingId && archive.archiveProxy().publication().isConnected())
        {
            try
            {
                archive.tryStopRecordingByIdentity(logRecordingId);
            }
            catch (final Exception ex)
            {
                ctx.countedErrorHandler().onError(new ClusterException(ex, WARN));
            }
        }
    }

    private long getLastAppendedPosition()
    {
        idleStrategy.reset();
        while (true)
        {
            final long appendPosition = archive.getStopPosition(logRecordingId);
            if (NULL_POSITION != appendPosition)
            {
                return appendPosition;
            }

            idle();
        }
    }

    private void appendDynamicJoinTermAndSnapshots()
    {
        if (!dynamicJoinSnapshots.isEmpty())
        {
            final RecordingLog.Snapshot lastSnapshot = dynamicJoinSnapshots.get(dynamicJoinSnapshots.size() - 1);

            recordingLog.appendTerm(
                logRecordingId,
                lastSnapshot.leadershipTermId,
                lastSnapshot.termBaseLogPosition,
                lastSnapshot.timestamp);

            for (int i = dynamicJoinSnapshots.size() - 1; i >= 0; i--)
            {
                final RecordingLog.Snapshot snapshot = dynamicJoinSnapshots.get(i);

                recordingLog.appendSnapshot(
                    snapshot.recordingId,
                    snapshot.leadershipTermId,
                    snapshot.termBaseLogPosition,
                    snapshot.logPosition,
                    snapshot.timestamp,
                    snapshot.serviceId);
            }

            dynamicJoinSnapshots.clear();
        }
    }

    private boolean isIngressMulticast()
    {
        final ChannelUri ingressUri = ChannelUri.parse(ctx.ingressChannel());

        if (!ingressUri.containsKey(ENDPOINT_PARAM_NAME))
        {
            ingressUri.put(ENDPOINT_PARAM_NAME, thisMember.ingressEndpoint());
        }

        final InetSocketAddress addr = UdpChannel.destinationAddress(ingressUri, DefaultNameResolver.INSTANCE);

        // assume that if not resolved is a non-multicast address
        return null != addr && null != addr.getAddress() && addr.getAddress().isMulticastAddress();
    }

    private void connectIngress()
    {
        final ChannelUri ingressUri = ChannelUri.parse(ctx.ingressChannel());
        final boolean isIngressMulticast = isIngressMulticast();

        if (Cluster.Role.LEADER != role && isIngressMulticast)
        {
            // don't subscribe to ingress if follower and multicast ingress
            return;
        }

        String ingressNetworkEndpoint = ingressUri.get(ENDPOINT_PARAM_NAME);
        final String ingressNetworkInterface = ingressUri.get(INTERFACE_PARAM_NAME);
        if (null == ingressNetworkEndpoint)
        {
            ingressNetworkEndpoint = thisMember.ingressEndpoint();
        }

        ingressUri.remove(ENDPOINT_PARAM_NAME);
        ingressUri.remove(INTERFACE_PARAM_NAME);
        ingressUri.put(MDC_CONTROL_MODE_PARAM_NAME, MDC_CONTROL_MODE_MANUAL);

        final Subscription ingressSubscription = aeron.addSubscription(
            ingressUri.toString(), ctx.ingressStreamId(), null, this::onUnavailableIngressImage);

        final String ingressNetworkDestination = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .endpoint(ingressNetworkEndpoint)
            .networkInterface(ingressNetworkInterface)
            .build();

        ingressSubscription.addDestination(ingressNetworkDestination);

        if (ctx.isIpcIngressAllowed() && Cluster.Role.LEADER == role)
        {
            ingressSubscription.addDestination(IPC_CHANNEL);
        }

        ingressAdapter.connect(ingressSubscription);
    }

    private void ensureConsistentInitialTermId(final ChannelUri channelUri)
    {
        channelUri.put(INITIAL_TERM_ID_PARAM_NAME, "0");
        channelUri.put(TERM_ID_PARAM_NAME, "0");
        channelUri.put(TERM_OFFSET_PARAM_NAME, "0");
    }

    private void checkFollowerForConsensusPublication(final int followerMemberId)
    {
        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
        if (null != follower && null == follower.publication())
        {
            ClusterMember.addConsensusPublication(
                follower, ctx.consensusChannel(), ctx.consensusStreamId(), aeron, ctx.countedErrorHandler());
        }
    }

    private void runTerminationHook()
    {
        try
        {
            ctx.terminationHook().run();
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }
    }

    private String createResponseChannel(final String responseChannel)
    {
        final String egressChannel = ctx.egressChannel();
        if (null == egressChannel)
        {
            return responseChannel; // legacy behavior
        }
        else if (responseChannel.contains(ENDPOINT_PARAM_NAME))
        {
            final String responseEndpoint = ChannelUri.parse(responseChannel).get(ENDPOINT_PARAM_NAME);
            final ChannelUri channel = ChannelUri.parse(egressChannel);
            channel.put(ENDPOINT_PARAM_NAME, responseEndpoint);
            return channel.toString();
        }
        else if (ctx.isIpcIngressAllowed() && responseChannel.startsWith(IPC_CHANNEL))
        {
            return responseChannel;
        }
        else
        {
            return egressChannel;
        }
    }

    private static boolean isCatchupAppendPosition(final short flags)
    {
        return 0 != (APPEND_POSITION_FLAG_CATCHUP & flags);
    }

    public String toString()
    {
        return "ConsensusModuleAgent{" +
            "election=" + election +
            '}';
    }
}
