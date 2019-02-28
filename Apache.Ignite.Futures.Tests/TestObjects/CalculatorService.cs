using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Core.Services;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    class CalculatorService : ICalculator, IService
    {
        public Task<int> Sum(int n1, int n2, int duration, CancellationToken ct)
        {
            throw new System.NotImplementedException();
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
