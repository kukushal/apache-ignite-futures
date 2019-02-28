using Apache.Ignite.Core.Messaging;
using System;
using System.Threading;

namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Client-side processing of <see cref="TopicMessageFuture"/> messages.
    /// </summary>
    internal class MessageListener : IMessageListener<object>
    {
        private readonly IMessaging igniteMsg;

        private readonly dynamic futureResult;
        private readonly TopicMessageFuture future;
        private readonly CancellationToken cancellation;

        /// <summary>
        /// Constructor.
        /// </summary>
        public MessageListener(
            IMessaging igniteMsg,
            dynamic futureResult,
            CancellationToken cancellation,
            TopicMessageFuture future)
        {
            this.igniteMsg = igniteMsg;
            this.futureResult = futureResult;
            this.future = future;
            this.cancellation = cancellation;

            if (future.State == State.Done)
                futureResult.SetResult((dynamic)future.Result);
            else
            {
                // Send cancellation request to the server if user cancels the async operation
                cancellation.Register(() => igniteMsg.Send(new CancelReq(), future.Topic));

                igniteMsg.Send(new ResultReq(), future.Topic);
            }
        }

        /// <summary>
        /// Process Ignite topic-based message coming from the server's <see cref="TopicMessageFuture"/>.
        /// </summary>
        public bool Invoke(Guid nodeId, object msg)
        {
            switch (msg)
            {
                case Result res:
                    futureResult.SetResult((dynamic)res.Value);
                    return false; // stop listening

                case CancelAck cancelAck:
                    futureResult.SetCanceled();
                    return false;
            }

            return true; // continue listening
        }
    }
}
