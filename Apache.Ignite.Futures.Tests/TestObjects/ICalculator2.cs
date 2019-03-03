using Apache.Ignite.Futures.TopicMessage;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    public interface ICalculator2
    {
        TopicMessageFuture Sum(int n1, int n2, int duration);
    }
}
