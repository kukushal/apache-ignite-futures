using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    /// <summary>
    /// Calculator interface. For test purposes includes both sync and async operations to make sure mixed sync/async
    /// service invokcation works.
    /// </summary>
    public interface ICalculator
    {
        /// <summary>
        /// Async operation to add two integers.
        /// </summary>
        Task<int> sum(int n1, int n2, int duration, string failureMsg, CancellationToken ct);

        /// <summary>
        /// Sync operation to check whether the last async operation was cancelled.
        /// </summary>
        /// <returns>
        /// <code>true</code> if the async last operation was cancelled; <code>false</code> otherwise.
        /// </returns>
        bool wasCancelled();
    }
}
