using System;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core.Services;
using Apache.Ignite.Futures.TopicMessage;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    class CalculatorService2 : CalculatorService, ICalculator2
    {
        public TopicMessageFuture Sum(int n1, int n2, int duration)
        {


            var cancelSrc = new CancellationTokenSource();

            var task = Sum(n1, n2, duration, cancelSrc.Token);

            var fut = new TopicMessageFuture
            {
                Topic = Guid.NewGuid().ToString(),
                State = State.Init
            };

            

            task.ContinueWith(t =>
            {

            });

            return fut;
        }
    }
}
