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
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.service.Cluster;
import org.agrona.CloseHelper;

/**
 * replay logs for leader and follower nodes.
 */
final class LogReplay
{
    private final long startPosition;
    private final long stopPosition;
    private final int logSessionId;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final ConsensusModule.Context ctx;

    private final LogAdapter logAdapter;

    private final Subscription logSubscription;

    LogReplay(
        final AeronArchive archive,
        final long recordingId,
        final long startPosition,
        final long stopPosition,
        final LogAdapter logAdapter,
        final ConsensusModule.Context ctx)
    {
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.logAdapter = logAdapter;
        this.consensusModuleAgent = logAdapter.consensusModuleAgent();
        this.ctx = ctx;

        final String channel = ctx.replayChannel();
        final int streamId = ctx.replayStreamId();
        final long length = stopPosition - startPosition;
        logSessionId = (int)archive.startReplay(recordingId, startPosition, length, channel, streamId);
        logSubscription = ctx.aeron().addSubscription(ChannelUri.addSessionId(channel, logSessionId), streamId);
    }

    void close()
    {
        logAdapter.disconnect(ctx.countedErrorHandler());
        CloseHelper.close(ctx.countedErrorHandler(), logSubscription);
    }

    int doWork()
    {
        int workCount = 0;

        //if logAdapter's image is null, then set the image and wait for service to join as follower.
        if (null == logAdapter.image())
        {
            final Image image = logSubscription.imageBySessionId(logSessionId);
            if (null != image)
            {
                if (image.joinPosition() != startPosition)
                {
                    throw new ClusterException(
                        "joinPosition=" + image.joinPosition() + " expected startPosition=" + startPosition,
                        ClusterException.Category.WARN);
                }

                logAdapter.image(image);

                //wait for services to join the log as follower, and ack its position.
                //it does not wait for the service module to replay all the logs,
                //the logs in service module are replayed when doWork is called,
                //and there is no state in service module, it just joins an active log from a position and poll the logs
                //from that position.
                //TODO: how to tell if a service finishes replaying old raft logs, by stopPosition ?
                consensusModuleAgent.awaitServicesReady(
                    logSubscription.channel(),
                    logSubscription.streamId(),
                    logSessionId,
                    startPosition,
                    stopPosition,
                    true,
                    Cluster.Role.FOLLOWER);  //why as follower??

                workCount += 1;
            }
        }
        else
        {
            //replay the log in consensus module,
            // because all the logs need to be handled both in consensus and service modules.
            workCount += consensusModuleAgent.replayLogPoll(logAdapter, stopPosition);
        }

        return workCount;
    }

    /**
     * check if the log replay process has finished.
     * @return
     */
    boolean isDone()
    {
        //TODO: why can't be SNAPSHOT?
        return logAdapter.image() != null &&
            logAdapter.position() >= stopPosition &&
            consensusModuleAgent.state() != ConsensusModule.State.SNAPSHOT;
    }
}
