namespace Apache.Ignite.Futures.Tests.TestObjects
{
    public interface ICalculator
    {
        TopicMessageFuture<int> sum(int n1, int n2);

        bool wasCancelled();
    }
}
