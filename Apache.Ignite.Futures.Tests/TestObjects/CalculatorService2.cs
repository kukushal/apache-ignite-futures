using System;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Resource;
using Apache.Ignite.Core.Services;
using Apache.Ignite.Futures.TopicMessage;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    class CalculatorService2 : CalculatorService, ICalculator2
    {
        [InstanceResource]
        private readonly IIgnite ignite;

        public TopicMessageFuture sum(int n1, int n2, int duration)
        {
            var cancelSrc = new CancellationTokenSource();

            var task = Sum(n1, n2, duration, cancelSrc.Token);

            var fut = new TopicMessageFuture
            {
                Topic = Guid.NewGuid().ToString(),
                State = State.Init
            };

            var igniteMsg = ignite.GetMessaging();

            var lsnr = new ServerSideHandler(igniteMsg, fut);

            igniteMsg.LocalListen(lsnr, fut.Topic);

            task.ContinueWith(t =>
            {
                lsnr.Resolve(t.Result, 600_000);
            });

            return fut;
        }
    }
}
