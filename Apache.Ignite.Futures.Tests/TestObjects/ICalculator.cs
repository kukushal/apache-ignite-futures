namespace Apache.Ignite.Futures.Tests.TestObjects
{
    public interface ICalculator
    {
        TopicMessageFuture sum(int n1, int n2);

        bool wasCancelled();
    }
}
