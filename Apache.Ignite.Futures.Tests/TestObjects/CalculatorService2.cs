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

        public TopicMessageFuture sum(int n1, int n2, int duration, string failureMsg)
        {
            var cancelSrc = new CancellationTokenSource();

            var task = sum(n1, n2, duration, failureMsg, cancelSrc.Token);

            var lsnr = new ServerSideHandler<int>(ignite, task, cancelSrc);

            return lsnr.Future;
        }
    }
}
