using System;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core.Services;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    public class CalculatorService : ICalculator, IService
    {
        private bool cancelled = false;

        public Task<int> sum(int n1, int n2, int duration, string failureMsg, CancellationToken ct)
        {
            cancelled = true;
            Exception failure = failureMsg == null ? null : new Exception(failureMsg);

            if (duration > 0)
                return Task.Run<int>(() =>
                    {
                        const int IterationsCnt = 10;

                        if (duration > IterationsCnt)
                        {
                            for (int i = 0; i < IterationsCnt && !ct.IsCancellationRequested; i++)
                                Thread.Sleep(duration / IterationsCnt);
                        }
                        else
                            Thread.Sleep(duration);

                        cancelled = ct.IsCancellationRequested;

                        ct.ThrowIfCancellationRequested();

                        if (failure != null)
                            throw failure;

                        return n1 + n2;
                    });

            return failure == null ? Task.FromResult<int>(n1 + n2) : Task.FromException<int>(failure);
        }

        public bool wasCancelled()
        {
            return cancelled;
        }

        public void Cancel(IServiceContext context)
        {
        }

        public void Execute(IServiceContext context)
        {
        }

        public void Init(IServiceContext context)
        {
        }
    }
}
