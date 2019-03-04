using Apache.Ignite.Core.Messaging;
using System;
using System.Threading;

namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Server-side processing of <see cref="TopicMessageFuture"/> messages.
    /// </summary>
    public class ServerSideHandler : IMessageListener<object>
    {
        private readonly IMessaging igniteMsg;
        private readonly TopicMessageFuture future;

        private readonly EventWaitHandle clientReadyEvent = new ManualResetEvent(false);

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServerSideHandler(IMessaging igniteMsg, TopicMessageFuture future)
        {
            this.igniteMsg = igniteMsg;
            this.future = future;
        }

        /// <summary>
        /// Process Ignite topic-based message coming from the client's <see cref="TopicMessageFuture"/>.
        /// </summary>
        public bool Invoke(Guid nodeId, object msg)
        {
            switch (msg)
            {
                case ResultReq resReq:
                    clientReadyEvent.Set();
                    break;
            }

            return true; // continue listening
        }

        public void Resolve(object result, int resolveTimeout)
        {
            if (!clientReadyEvent.WaitOne(resolveTimeout))
            {
                throw new TimeoutException("TopicMessageFuture resolution timed out.");
            }

            igniteMsg.Send(new Result { Value = result }, future.Topic);
        }
    }
}
