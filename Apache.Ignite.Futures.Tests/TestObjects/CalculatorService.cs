using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core.Services;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    class CalculatorService : ICalculator, IService
    {
        public Task<int> sum(int n1, int n2, int duration, CancellationToken ct)
        {
            return duration > 0
                ? Task.Run<int>(() =>
                {
                    const int IterationsCnt = 10;

                    if (duration > IterationsCnt)
                    {
                        for (int i = 0; i < IterationsCnt && !ct.IsCancellationRequested; i++)
                            Thread.Sleep(duration / IterationsCnt);
                    }
                    else
                        Thread.Sleep(duration);

                    ct.ThrowIfCancellationRequested();

                    return n1 + n2;
                })
                : Task.FromResult<int>(n1 + n2);
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
