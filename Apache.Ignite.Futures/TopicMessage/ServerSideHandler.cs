﻿using Apache.Ignite.Core;
using Apache.Ignite.Core.Messaging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Server-side processing of <see cref="TopicMessageFuture"/> messages.
    /// <para/>
    /// Keep it public: the class is used by a custom service dynamically generated in <see cref="ServiceDeployer"/>.
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
        public ServerSideHandler(IIgnite ignite, Task<T> task, CancellationTokenSource cancellation)
        {
            igniteMsg = ignite.GetMessaging();
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
                Future.Result = ToResult(task);
            else
            {
                task.ContinueWith(t =>
                {
                    try
                    {
                        Resolve(120_000); // 2 minutes: hard-coded resolution timeout
                    }
                    catch (OperationCanceledException)
                    {
                        // Ignore: the cancellation was already processed in the CancelReq handler.
                    }
                });

                igniteMsg.LocalListen(this, Future.Topic);
            }
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

        private void Resolve(int resolveTimeout)
        {
            if (!clientReadyEvent.WaitOne(resolveTimeout))
            {
                throw new TimeoutException("TopicMessageFuture resolution timed out.");
            }

            igniteMsg.Send(ToResult(task), Future.Topic);
        }

        private static Result ToResult(Task<T> task)
        {
            try
            {
                return new Result { Value = task.Result };
            }
            catch (AggregateException ex)
            {
                return new Result { Failure = ex.ToString() };
            }
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

                case TaskStatus.Faulted:
                    return State.Failed;

                default:
                    return State.Init;
            }
        }
    }
}
