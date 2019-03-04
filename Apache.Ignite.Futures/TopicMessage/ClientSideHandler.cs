using Apache.Ignite.Core.Messaging;
using System;
using System.Threading;

namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Client-side processing of <see cref="TopicMessageFuture"/> messages.
    /// </summary>
    internal class ClientSideHandler : IMessageListener<object>
    {
        private readonly IMessaging igniteMsg;

        private readonly dynamic futureResult;
        private readonly TopicMessageFuture future;
        private readonly CancellationToken cancellation;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ClientSideHandler(
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
                SetResult(future.Result);
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
        /// <returns><code>true</code> to keep the loop; <code>false</code> to stop messages processing.</returns>
        public bool Invoke(Guid nodeId, object msg)
        {
            switch (msg)
            {
                case Result res:
                    SetResult(res);
                    return false; // stop listening

                case CancelAck cancelAck:
                    futureResult.SetCanceled();
                    return false;
            }

            return true; // continue listening
        }

        private void SetResult(Result res)
        {
            if (res.Failure != null)
                futureResult.SetException((dynamic)new ServiceException(res.Failure));
            else
                futureResult.SetResult((dynamic)res.Value);
        }
    }
}
