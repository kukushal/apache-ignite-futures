using Apache.Ignite.Core.Messaging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Server-side processing of <see cref="TopicMessageFuture"/> messages.
    /// </summary>
    public class ServerSideHandler<T> : IMessageListener<object>
    {
        private readonly IMessaging igniteMsg;
        private readonly Task<T> task;
        private readonly CancellationTokenSource cancellation;

        private readonly EventWaitHandle clientReadyEvent = new ManualResetEvent(false);

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServerSideHandler(IMessaging igniteMsg, Task<T> task, CancellationTokenSource cancellation)
        {
            this.igniteMsg = igniteMsg;
            this.task = task;
            this.cancellation = cancellation;

            var state = ToState(task.Status);

            Future = new TopicMessageFuture
            {
                Topic = Guid.NewGuid().ToString(),
                State = state
            };

            if (cancellation.Token.CanBeCanceled)
                Future.CancelTimeout = 120_000; // 2 minutes: hard-coded cancellation timeout

            if (state == State.Done)
                Future.Result = task.Result;
            else
                task.ContinueWith(t => 
                {
                    try
                    {
                        Resolve(t.Result, 600_000);
                    }
                    catch (OperationCanceledException)
                    {
                        // Ignore: the cancellation was already processed in the CancelReq handler.
                    }
                });
        }

        public TopicMessageFuture Future { get; private set; }

        /// <summary>
        /// Process Ignite topic-based message coming from the client's <see cref="TopicMessageFuture"/>.
        /// </summary>
        /// <returns><code>true</code> to keep the loop; <code>false</code> to stop messages processing.</returns>
        public bool Invoke(Guid nodeId, object msg)
        {
            bool isFinalMsg = false;

            switch (msg)
            {
                case ResultReq resReq:
                    clientReadyEvent.Set();
                    break;

                case CancelReq cancelReq:
                    string failure = null;

                    try
                    {
                        cancellation.Cancel();

                        Future.State = State.Cancelled;
                        isFinalMsg = true;
                    }
                    catch (Exception ex)
                    {
                        failure = ex.Message;
                    }

                    igniteMsg.Send(new CancelAck { Failure = failure }, Future.Topic);

                    break;

                case Result res:
                    isFinalMsg = true; // unsubscribe on sending the result to the client
                    break;

                default:
                    isFinalMsg = true;
                    break;
            }

            return !isFinalMsg; 
        }

        private void Resolve(object result, int resolveTimeout)
        {
            if (!clientReadyEvent.WaitOne(resolveTimeout))
            {
                throw new TimeoutException("TopicMessageFuture resolution timed out.");
            }

            igniteMsg.Send(new Result { Value = result }, Future.Topic);
        }

        private static State ToState(TaskStatus taskStatus)
        {
            switch (taskStatus)
            {
                case TaskStatus.RanToCompletion:
                    return State.Done;

                case TaskStatus.Canceled:
                    return State.Cancelled;

                case TaskStatus.Running:
                case TaskStatus.WaitingForChildrenToComplete:
                    return State.Active;

                default:
                    return State.Init;
            }
        }
    }
}
