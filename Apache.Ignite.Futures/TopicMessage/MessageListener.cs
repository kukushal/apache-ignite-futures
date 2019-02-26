using Apache.Ignite.Core.Messaging;
using System;

namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Client-side processing of <see cref="TopicMessageFuture"/> events.
    /// </summary>
    internal class MessageListener : IMessageListener<object>
    {
        private readonly dynamic futureResult;
        private readonly TopicMessageFuture future;

        /// <summary>
        /// Constructor.
        /// </summary>
        public MessageListener(dynamic futureResult, TopicMessageFuture future)
        {
            this.futureResult = futureResult;
            this.future = future;

            if (future.State == State.Done)
                futureResult.SetResult(future.Result);
        }

        /// <summary>
        /// Process Ignite topic-based message coming from the server's <see cref="TopicMessageFuture"/>.
        /// </summary>
        public bool Invoke(Guid nodeId, object msg)
        {
            switch (msg)
            {
                case Result res:
                    futureResult.SetResult(res.Value);
                    return false; // stop listening

                case CancelAck cancelAck:
                    // TODO: handle cancellation
                    return false;
            }

            return true; // continue listening
        }
    }
}
