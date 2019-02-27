using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    public interface ICalculator
    {
        Task<int> Sum(int n1, int n2, int duration, CancellationToken ct);
    }
}
