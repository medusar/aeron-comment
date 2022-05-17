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
package io.aeron.cluster.service;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.driver.Configuration;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ReadableCounter;
import org.agrona.*;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.CountersReader;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.*;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

/**
 * ClusteredService Agent
 * 1. Poll sessions related log through logAdapter, and decode messages and callback on ClusteredService.
 * 2. Poll ServiceTerminationPosition every millisecond and JoinLog through serviceAdapter,
 *  and callback on ClusteredServiceAgent itself.
 * 3. Send messages like scheduleTimer, closeSession etc. to consensus-module through ConsensusModuleProxy.
 */
final class ClusteredServiceAgent implements Agent, Cluster, IdleStrategy
{
    static final long MARK_FILE_UPDATE_INTERVAL_MS = TimeUnit.NANOSECONDS.toMillis(MARK_FILE_UPDATE_INTERVAL_NS);

    private volatile boolean isAbort;

    //Flag indicates if current service is active.
    private boolean isServiceActive;

    private final int serviceId;
    private int memberId = NULL_VALUE;
    private long closeHandlerRegistrationId;
    private long ackId = 0;
    private long terminationPosition = NULL_POSITION;
    private long markFileUpdateDeadlineMs;
    private long cachedTimeMs;

    //time of the cluster, updated when a log message is called back from logAdapter.
    private long clusterTime;

    //Log position current service has processed.
    private long logPosition = NULL_POSITION;

    private final IdleStrategy idleStrategy;

    //MarkFile for the cluster, mainly used for updateActivityTimestamp and memberId in this class.
    //update every 1 second. activityTimestamp will also be updated in ConsensusModuleAgent every 1 sec.
    private final ClusterMarkFile markFile;

    private final ClusteredServiceContainer.Context ctx;

    private final Aeron aeron;
    //ClientConductor invoker.
    private final AgentInvoker aeronAgentInvoker;

    //User implemented ClusteredService.
    private final ClusteredService service;

    //To send messages to consensus-module.
    private final ConsensusModuleProxy consensusModuleProxy;
    //To poll control messages from the consensus module.
    private final ServiceAdapter serviceAdapter;

    private final EpochClock epochClock;

    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(
        new byte[Configuration.MAX_UDP_PAYLOAD_LENGTH - DataHeaderFlyweight.HEADER_LENGTH]);
    private final DirectBufferVector headerVector = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
    private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();

    //Store all the sessions.
    private final Long2ObjectHashMap<ContainerClientSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final Collection<ClientSession> unmodifiableClientSessions =
        new UnmodifiableClientSessionCollection(sessionByIdMap.values());

    //To poll raft logs.
    private final BoundedLogAdapter logAdapter;

    //Temporary variable to indicate where some method is called from.
    private String activeLifecycleCallbackName;

    //Current commit position for the Raft log.
    //Will be used for logAdapter.poll, so that polled raft logs will not exceed the commitPosition.
    //This value will only be read here. It will be updated in consensus-module.
    private ReadableCounter commitPosition;

    //Temporary variable indicates a new ActiveLogEvent need to be handled.
    private ActiveLogEvent activeLogEvent;

    //Current role on the node.
    private Role role = Role.FOLLOWER;
    private TimeUnit timeUnit = null;

    ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {

        //to poll raft log
        logAdapter = new BoundedLogAdapter(this, ctx.logFragmentLimit());

        this.ctx = ctx;

        markFile = ctx.clusterMarkFile();
        aeron = ctx.aeron();

        //Aeron ClientConductor, invoked every millisecond in #checkForClockTick.
        aeronAgentInvoker = ctx.aeron().conductorAgentInvoker();
        //User implemented ClusteredService
        service = ctx.clusteredService();
        idleStrategy = ctx.idleStrategy();
        //Each ClusteredService has an id, the default value is 0.
        serviceId = ctx.serviceId();
        epochClock = ctx.epochClock();

        //This channel is used both for publishing to consensus-module and subscribing ServiceTerminationPosition related messages.
        final String channel = ctx.controlChannel();

        //Communicating with consensus-module
        consensusModuleProxy = new ConsensusModuleProxy(aeron.addPublication(channel, ctx.consensusModuleStreamId()));
        //poll ServiceTerminationPosition every millisecond and JoinLog through serviceAdapter
        serviceAdapter = new ServiceAdapter(aeron.addSubscription(channel, ctx.serviceStreamId()), this);
        sessionMessageHeaderEncoder.wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());
    }

    public void onStart()
    {
        closeHandlerRegistrationId = aeron.addCloseHandler(this::abort);

        //get the commit position counter
        final CountersReader counters = aeron.countersReader();
        commitPosition = awaitCommitPositionCounter(counters, ctx.clusterId());

        //Recover state based on cluster counters.
        recoverState(counters);
    }

    public void onClose()
    {
        aeron.removeCloseHandler(closeHandlerRegistrationId);

        if (isAbort)
        {
            ctx.abortLatch().countDown();
        }
        else
        {
            final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
            if (isServiceActive)
            {
                isServiceActive = false;
                try
                {
                    service.onTerminate(this);
                }
                catch (final Exception ex)
                {
                    errorHandler.onError(ex);
                }
            }

            if (!ctx.ownsAeronClient() && !aeron.isClosed())
            {
                for (final ContainerClientSession session : sessionByIdMap.values())
                {
                    session.disconnect(errorHandler);
                }

                CloseHelper.close(errorHandler, logAdapter);
                CloseHelper.close(errorHandler, serviceAdapter);
                CloseHelper.close(errorHandler, consensusModuleProxy);
            }
        }

        markFile.updateActivityTimestamp(NULL_VALUE);
        ctx.close();
    }

    /**
     * 1. checkForClockTick and pollServiceAdapter
     * 2. poll logs through logAdapter
     */
    public int doWork()
    {
        int workCount = 0;

        try
        {
            //Try to:
            //1. invoke aeron ClientConductor.doWork through aeronAgentInvoker
            //2. update cluster markFile activityTime.
            if (checkForClockTick())
            {
                //invoke serviceAdapter.poll() whose messages will be called back on this class.
                pollServiceAdapter();
                workCount += 1;
            }

            if (null != logAdapter.image())
            {
                //Poll Raft logs up to commitPosition, whose messages will be called back on this class.
                final int polled = logAdapter.poll(commitPosition.get());
                workCount += polled;

                if (0 == polled && logAdapter.isDone())
                {
                    closeLog();
                }
            }
        }
        catch (final AgentTerminationException ex)
        {
            runTerminationHook();
            throw ex;
        }

        return workCount;
    }

    public String roleName()
    {
        return ctx.serviceName();
    }

    public Cluster.Role role()
    {
        return role;
    }

    public int memberId()
    {
        return memberId;
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public ClusteredServiceContainer.Context context()
    {
        return ctx;
    }

    public ClientSession getClientSession(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    public Collection<ClientSession> clientSessions()
    {
        return unmodifiableClientSessions;
    }

    public void forEachClientSession(final Consumer<? super ClientSession> action)
    {
        sessionByIdMap.values().forEach(action);
    }

    public boolean closeClientSession(final long clusterSessionId)
    {
        checkForLifecycleCallback();

        final ContainerClientSession clientSession = sessionByIdMap.get(clusterSessionId);
        if (clientSession == null)
        {
            throw new ClusterException("unknown clusterSessionId: " + clusterSessionId);
        }

        if (clientSession.isClosing())
        {
            return true;
        }

        if (consensusModuleProxy.closeSession(clusterSessionId))
        {
            clientSession.markClosing();
            return true;
        }

        return false;
    }

    public TimeUnit timeUnit()
    {
        return timeUnit;
    }

    public long time()
    {
        return clusterTime;
    }

    public long logPosition()
    {
        return logPosition;
    }

    public boolean scheduleTimer(final long correlationId, final long deadline)
    {
        checkForLifecycleCallback();

        return consensusModuleProxy.scheduleTimer(correlationId, deadline);
    }

    public boolean cancelTimer(final long correlationId)
    {
        checkForLifecycleCallback();

        return consensusModuleProxy.cancelTimer(correlationId);
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        checkForLifecycleCallback();
        sessionMessageHeaderEncoder.clusterSessionId(0);

        return consensusModuleProxy.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
    }

    public long offer(final DirectBufferVector[] vectors)
    {
        checkForLifecycleCallback();
        sessionMessageHeaderEncoder.clusterSessionId(0);
        vectors[0] = headerVector;

        return consensusModuleProxy.offer(vectors);
    }

    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        checkForLifecycleCallback();
        sessionMessageHeaderEncoder.clusterSessionId(0);

        return consensusModuleProxy.tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim, headerBuffer);
    }

    public IdleStrategy idleStrategy()
    {
        return this;
    }

    public void reset()
    {
        idleStrategy.reset();
    }

    public void idle()
    {
        idleStrategy.idle();
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("interrupted");
        }
        checkForClockTick();
    }

    public void idle(final int workCount)
    {
        idleStrategy.idle(workCount);
        if (workCount <= 0)
        {
            if (Thread.currentThread().isInterrupted())
            {
                throw new AgentTerminationException("interrupted");
            }
            checkForClockTick();
        }
    }

    //called back from ServiceAdapter
    //JoinLog
    //When consensus module joint a log, it sends a message.
    void onJoinLog(
        final long logPosition,
        final long maxLogPosition,
        final int memberId,
        final int logSessionId, //the publication.sessionId for log Publication.
        final int logStreamId,
        final boolean isStartup,
        final Cluster.Role role,
        final String logChannel)
    {
        //the logPosition the consensus module has consumed.
        logAdapter.maxLogPosition(logPosition);

        //create activeLogEvent and other logic will be processed in `pollServiceAdapter`
        activeLogEvent = new ActiveLogEvent(
            logPosition,
            maxLogPosition,
            memberId,
            logSessionId,
            logStreamId,
            isStartup,
            role,
            logChannel);
    }

    //Called back from ServiceAdapter
    //set the terminationPosition, and other logic will be processed in `pollServiceAdapter`
    void onServiceTerminationPosition(final long logPosition)
    {
        terminationPosition = logPosition;
    }

    //Called back from logAdapter
    //invoked when a message is received from client by consensus-module.
    void onSessionMessage(
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,    //timestamp in the consensus-module.
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        //update logPosition and clusterTime
        this.logPosition = logPosition;
        clusterTime = timestamp;
        //find the clientSession and invoke user implemented ClusteredService
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);

        service.onSessionMessage(clientSession, timestamp, buffer, offset, length, header);
    }

    //Called back from logAdapter
    //invoked when a timer event is received
    void onTimerEvent(final long logPosition, final long correlationId, final long timestamp)
    {
        //update logPosition and clusterTime
        this.logPosition = logPosition;
        clusterTime = timestamp;
        //invoke user implemented ClusteredService
        service.onTimerEvent(correlationId, timestamp);
    }

    //Called back from logAdapter
    //invoked when a session is opened.
    void onSessionOpen(
        final long leadershipTermId, //the leader termid when connected
        final long logPosition,
        final long clusterSessionId,  //sessionId
        final long timestamp,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;

        if (sessionByIdMap.containsKey(clusterSessionId))
        {
            throw new ClusterException("clashing open clusterSessionId=" + clusterSessionId +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
        }

        final ContainerClientSession session = new ContainerClientSession(
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

        if (Role.LEADER == role && ctx.isRespondingService())
        {
            session.connect(aeron);
        }

        sessionByIdMap.put(clusterSessionId, session);
        service.onSessionOpen(session, timestamp);
    }

    //Called back from logAdapter
    void onSessionClose(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final CloseReason closeReason)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;
        final ContainerClientSession session = sessionByIdMap.remove(clusterSessionId);

        if (null == session)
        {
            throw new ClusterException(
                "unknown clusterSessionId=" + clusterSessionId + " for close reason=" + closeReason +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
        }

        session.disconnect(ctx.countedErrorHandler());
        service.onSessionClose(session, timestamp, closeReason);
    }

    //Called back from logAdapter
    //ClusterActionRequest, for now, only SNAPSHOT is supported.
    void onServiceAction(
        final long leadershipTermId, final long logPosition, final long timestamp, final ClusterAction action)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;
        executeAction(action, logPosition, leadershipTermId);
    }

    //Called back from logAdapter
    //invoked when a new leadership term is created
    void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        if (SemanticVersion.major(ctx.appVersion()) != SemanticVersion.major(appVersion))
        {
            ctx.errorHandler().onError(new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " log=" + SemanticVersion.toString(appVersion)));
            throw new AgentTerminationException();
        }
        else
        {
            sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
            this.logPosition = logPosition;
            clusterTime = timestamp;
            this.timeUnit = timeUnit;

            //Invoke user implemented methods.
            service.onNewLeadershipTermEvent(
                leadershipTermId,
                logPosition,
                timestamp,
                termBaseLogPosition,
                leaderMemberId,
                logSessionId,
                timeUnit,
                appVersion);
        }
    }

    //Called back from logAdapter
    //invoked when a member join or quit the cluster.
    //ChangeType: JOIN, QUIT.
    void onMembershipChange(
        final long logPosition, final long timestamp, final ChangeType changeType, final int memberId)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;

        if (memberId == this.memberId && changeType == ChangeType.QUIT)
        {
            terminate(true);
        }
    }

    //Called back from ServiceSnapshotLoader
    //when a snapshot is loaded, it will recover all the sessions in the snapshot.
    //this method will add all the sessions into sessionByIdMap.
    void addSession(
        final long clusterSessionId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        sessionByIdMap.put(clusterSessionId, new ContainerClientSession(
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this));
    }

    void handleError(final Throwable ex)
    {
        ctx.countedErrorHandler().onError(ex);
    }

    long offer(
        final long clusterSessionId,
        final Publication publication,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        checkForLifecycleCallback();

        if (Cluster.Role.LEADER != role)
        {
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        sessionMessageHeaderEncoder
            .clusterSessionId(clusterSessionId)
            .timestamp(clusterTime);

        return publication.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);
    }

    long offer(final long clusterSessionId, final Publication publication, final DirectBufferVector[] vectors)
    {
        checkForLifecycleCallback();

        if (Cluster.Role.LEADER != role)
        {
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        sessionMessageHeaderEncoder
            .clusterSessionId(clusterSessionId)
            .timestamp(clusterTime);

        vectors[0] = headerVector;

        return publication.offer(vectors, null);
    }

    long tryClaim(
        final long clusterSessionId,
        final Publication publication,
        final int length,
        final BufferClaim bufferClaim)
    {
        checkForLifecycleCallback();

        if (Cluster.Role.LEADER != role)
        {
            final int maxPayloadLength = headerBuffer.capacity() - SESSION_HEADER_LENGTH;
            if (length > maxPayloadLength)
            {
                throw new IllegalArgumentException(
                    "claim exceeds maxPayloadLength=" + maxPayloadLength + ", length=" + length);
            }

            bufferClaim.wrap(headerBuffer, 0, length + SESSION_HEADER_LENGTH);
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        final long offset = publication.tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim);
        if (offset > 0)
        {
            sessionMessageHeaderEncoder
                .clusterSessionId(clusterSessionId)
                .timestamp(clusterTime);

            bufferClaim.putBytes(headerBuffer, 0, SESSION_HEADER_LENGTH);
        }

        return offset;
    }

    private void role(final Role newRole)
    {
        if (newRole != role)
        {
            role = newRole;
            activeLifecycleCallbackName = "onRoleChange";
            try
            {
                service.onRoleChange(newRole);
            }
            finally
            {
                activeLifecycleCallbackName = null;
            }
        }
    }

    //1. recover from counters, which means some state information is stored in counters,
    // these counters should not be changed during restart.
    //2. loadSnapshot based on counters.
    //TODO: where are the counters saved ?
    private void recoverState(final CountersReader counters)
    {
        final int recoveryCounterId = awaitRecoveryCounter(counters);

        //Read logPosition and clusterTime from counters.
        logPosition = RecoveryState.getLogPosition(counters, recoveryCounterId);
        clusterTime = RecoveryState.getTimestamp(counters, recoveryCounterId);

        //Read leadership termId from counters
        final long leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
        isServiceActive = true;

        activeLifecycleCallbackName = "onStart";
        try
        {
            //If there is leaderTermId, then load from snapshot
            if (NULL_VALUE != leadershipTermId)
            {
                //load snapshot and invoke user implemented service
                loadSnapshot(RecoveryState.getSnapshotRecordingId(counters, recoveryCounterId, serviceId));
            }
            else
            {
                //Load without snapshot
                service.onStart(this, null);
            }
        }
        finally
        {
            activeLifecycleCallbackName = null;
        }

        //to acknowledge the logPosition and clientId to consensus-module
        final long id = ackId++;
        idleStrategy.reset();
        while (!consensusModuleProxy.ack(logPosition, clusterTime, id, aeron.clientId(), serviceId))
        {
            idle();
        }
    }

    private int awaitRecoveryCounter(final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = RecoveryState.findCounterId(counters, ctx.clusterId());
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = RecoveryState.findCounterId(counters, ctx.clusterId());
        }

        return counterId;
    }

    private void closeLog()
    {
        logPosition = Math.max(logAdapter.image().position(), logPosition);
        CloseHelper.close(ctx.countedErrorHandler(), logAdapter);
        role(Role.FOLLOWER);
    }

    //To join an active raft log:
    //1. to set Image for logAdapter
    //2. to change role and memberId based on ActiveLogEvent
    //3. to disconnect or connect client sessions based on new role.
    private void joinActiveLog(final ActiveLogEvent activeLog)
    {
        //If current node is not leader, then close all the connected client sessions.
        //ClientSessions are only allowed to connect to leader node.
        //Note: sessions will only be disconnected here but not removed yet. Removing is performed elsewhere.
        if (Role.LEADER != activeLog.role)
        {
            for (final ContainerClientSession session : sessionByIdMap.values())
            {
                session.disconnect(ctx.countedErrorHandler());
            }
        }

        //Subscribe to raft log with specified channel and streamId.
        Subscription logSubscription = aeron.addSubscription(activeLog.channel, activeLog.streamId);
        try
        {
            //Wait for image to be ready.
            //TODO: why need to wait ? how much time will it take?
            final Image image = awaitImage(activeLog.sessionId, logSubscription);
            if (image.joinPosition() != logPosition)
            {
                throw new ClusterException("Cluster log must be contiguous for joining image: " +
                    "expectedPosition=" + logPosition + " joinPosition=" + image.joinPosition());
            }

            //logPosition must be contiguous
            if (activeLog.logPosition != logPosition)
            {
                throw new ClusterException("Cluster log must be contiguous for active log event: " +
                    "expectedPosition=" + logPosition + " eventPosition=" + activeLog.logPosition);
            }

            //setup for the logAdapter
            logAdapter.image(image);
            logAdapter.maxLogPosition(activeLog.maxLogPosition);
            logSubscription = null;

            //Acknowledge its logPosition to consensus-module. Retry until success.
            final long id = ackId++;
            idleStrategy.reset();
            while (!consensusModuleProxy.ack(activeLog.logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                idle();
            }
        }
        finally
        {
            //TODO:
            //Why close? Will Image still be available after subscription closed ?
            CloseHelper.quietClose(logSubscription);
        }

        //Change this memberId to activeLog.memberId, and update markFile.
        //That means, memberId could also be changed when cluster works.
        memberId = activeLog.memberId;
        markFile.memberId(memberId);

        //If current role will leader, then reconnect all the client sessions.
        if (Role.LEADER == activeLog.role)
        {
            for (final ContainerClientSession session : sessionByIdMap.values())
            {
                //ctx.isRespondingService() is normally true
                //TODO: What does activeLog.isStartup means ?
                if (ctx.isRespondingService() && !activeLog.isStartup)
                {
                    session.connect(aeron);
                }

                //Reset close flags.
                session.resetClosing();
            }
        }

        //Change role to activeLog.role.
        role(activeLog.role);
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

    private ReadableCounter awaitCommitPositionCounter(final CountersReader counters, final int clusterId)
    {
        idleStrategy.reset();
        int counterId = ClusterCounters.find(counters, COMMIT_POSITION_TYPE_ID, clusterId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = ClusterCounters.find(counters, COMMIT_POSITION_TYPE_ID, clusterId);
        }

        return new ReadableCounter(counters, counterId);
    }

    //load snapshot with given recordingId
    private void loadSnapshot(final long recordingId)
    {
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext().clone()))
        {
            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();

            //replay all the data in the recording to `replayChannel`
            final int sessionId = (int)archive.startReplay(recordingId, 0, NULL_VALUE, channel, streamId);

            final String replaySessionChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replaySessionChannel, streamId))
            {
                final Image image = awaitImage(sessionId, subscription);
                //load States in from snapshot, mainly client sessions.
                loadState(image, archive);
                //invoke user implemented methods.
                service.onStart(this, image);
            }
        }
    }

    //load ClientSessions from snapshots
    private void loadState(final Image image, final AeronArchive archive)
    {
        final ServiceSnapshotLoader snapshotLoader = new ServiceSnapshotLoader(image, this);
        while (true)
        {
            //poll and handle data from snapshots.
            final int fragments = snapshotLoader.poll();
            if (snapshotLoader.isDone())
            {
                break;
            }

            if (fragments == 0)
            {
                archive.checkForErrorResponse();
                if (image.isClosed())
                {
                    throw new ClusterException("snapshot ended unexpectedly: " + image);
                }
            }

            idle(fragments);
        }

        final int appVersion = snapshotLoader.appVersion();
        if (SemanticVersion.major(ctx.appVersion()) != SemanticVersion.major(appVersion))
        {
            throw new ClusterException(
                "incompatible app version: " + SemanticVersion.toString(ctx.appVersion()) +
                " snapshot=" + SemanticVersion.toString(appVersion));
        }

        timeUnit = snapshotLoader.timeUnit();
    }

    //Called back from logAdapter to onServiceAction
    private long onTakeSnapshot(final long logPosition, final long leadershipTermId)
    {
        //create a new publication each time a snapshot is to be taken.
        //A new recording id will be created each time a snapshot is taken.
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext().clone());
            ExclusivePublication publication = aeron.addExclusivePublication(
                ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {

            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL, true);

            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounter(publication.sessionId(), counters, archive);

            //A new recording id will be created each time a snapshot is taken.
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            //Recording states into snapshot, mainly client sessions.
            snapshotState(publication, logPosition, leadershipTermId);

            checkForClockTick();
            archive.checkForErrorResponse();

            //Invoke on user implemented service so that users can encode business info into snapshot.
            service.onTakeSnapshot(publication);

            //Wait for the archive recording the catchup to the publication position.
            awaitRecordingComplete(recordingId, publication.position(), counters, counterId, archive);

            return recordingId;
        }
        catch (final ArchiveException ex)
        {
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                throw new AgentTerminationException(ex);
            }

            throw ex;
        }
    }

    private void awaitRecordingComplete(
        final long recordingId,
        final long position,
        final CountersReader counters,
        final int counterId,
        final AeronArchive archive)
    {
        idleStrategy.reset();
        while (counters.getCounterValue(counterId) < position)
        {
            idle();
            archive.checkForErrorResponse();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording stopped unexpectedly: " + recordingId);
            }
        }
    }

    //Encode cluster state into snapshot, mainly client sesions.
    private void snapshotState(
        final ExclusivePublication publication, final long logPosition, final long leadershipTermId)
    {
        final ServiceSnapshotTaker snapshotTaker = new ServiceSnapshotTaker(
            publication, idleStrategy, aeronAgentInvoker);

        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, timeUnit, ctx.appVersion());

        for (final ClientSession clientSession : sessionByIdMap.values())
        {
            snapshotTaker.snapshotSession(clientSession);
        }

        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, timeUnit, ctx.appVersion());
    }

    private void executeAction(final ClusterAction action, final long logPosition, final long leadershipTermId)
    {
        if (ClusterAction.SNAPSHOT == action)
        {
            //do take snapshot, and return the recordingId for the snapshot archive recording.
            final long recordingId = onTakeSnapshot(logPosition, leadershipTermId);

            //acknowledge the consensus-module with the recordingId.
            final long id = ackId++;
            idleStrategy.reset();
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, recordingId, serviceId))
            {
                idle();
            }
        }
    }

    private int awaitRecordingCounter(final int sessionId, final CountersReader counters, final AeronArchive archive)
    {
        idleStrategy.reset();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            archive.checkForErrorResponse();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
        }

        return counterId;
    }

    /**
     * tick timer
     *
     * 1. invoke aeron ClientConductor through aeronAgentInvoker every 1 millisecond(by default)
     * 2. update cluster markFile activityTime
     *
     * @return if timer task has been invoked.
     */
    private boolean checkForClockTick()
    {
        if (isAbort || aeron.isClosed())
        {
            isAbort = true;
            throw new AgentTerminationException("unexpected Aeron close");
        }

        final long nowMs = epochClock.time();
        if (cachedTimeMs != nowMs)
        {
            cachedTimeMs = nowMs;

            if (null != aeronAgentInvoker)
            {
                aeronAgentInvoker.invoke();
                if (isAbort || aeron.isClosed())
                {
                    isAbort = true;
                    throw new AgentTerminationException("unexpected Aeron close");
                }
            }

            if (nowMs >= markFileUpdateDeadlineMs)
            {
                markFileUpdateDeadlineMs = nowMs + MARK_FILE_UPDATE_INTERVAL_MS;
                markFile.updateActivityTimestamp(nowMs);
            }

            return true;
        }

        return false;
    }

    //Poll from serviceAdapter
    //Poll messages and handle activeLogEvent and terminationPosition related logic.
    private void pollServiceAdapter()
    {
        //poll and callback on this class.
        serviceAdapter.poll();

        //if activeLogEvent is created
        //joinActiveLog
        if (null != activeLogEvent && null == logAdapter.image())
        {
            final ActiveLogEvent event = activeLogEvent;
            activeLogEvent = null;
            joinActiveLog(event);
        }

        //if terminationPosition is set and current logPosition reaches terminationPosition,
        //then termination this Service.
        if (NULL_POSITION != terminationPosition && logPosition >= terminationPosition)
        {
            if (logPosition > terminationPosition)
            {
                ctx.countedErrorHandler().onError(new ClusterEvent(
                    "service terminate: logPosition=" + logPosition + " > terminationPosition=" + terminationPosition));
            }

            terminate(logPosition == terminationPosition);
        }
    }

    //Terminate current service.
    //Will invoke user implemented ClusterService.onTerminate
    private void terminate(final boolean isTerminationExpected)
    {
        //Change flag to false
        isServiceActive = false;
        activeLifecycleCallbackName = "onTerminate";

        try
        {
            service.onTerminate(this);
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }
        finally
        {
            activeLifecycleCallbackName = null;
        }

        try
        {
            //Acknowledge the consensus-module, this ack will not be guaranteed success.
            int attempts = 5;
            final long id = ackId++;
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                if (0 == --attempts)
                {
                    break;
                }
                idle();
            }
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }

        terminationPosition = NULL_VALUE;

        //Throw ClusterTerminationException, which is a sub-class of  AgentTerminationException,
        //will terminate the AgentRunner.
        throw new ClusterTerminationException(isTerminationExpected);
    }

    private void checkForLifecycleCallback()
    {
        if (null != activeLifecycleCallbackName)
        {
            throw new ClusterException(
                "sending messages or scheduling timers is not allowed from " + activeLifecycleCallbackName);
        }
    }

    private void abort()
    {
        isAbort = true;

        try
        {
            if (!ctx.abortLatch().await(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L, TimeUnit.MILLISECONDS))
            {
                ctx.countedErrorHandler().onError(
                    new TimeoutException("awaiting abort latch", AeronException.Category.WARN));
            }
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
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
}
