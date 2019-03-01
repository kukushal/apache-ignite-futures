namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Ignite-Java async service calls return <see cref="TopicMessageFuture"/>. Keep the property names in sync with 
    /// the Java counterpart.
    /// </summary>
    internal class TopicMessageFuture
    {
        public string Topic { get; set; }

        public State State { get; set; }

        public object Result { get; set; }

        public long CancelTimeout { get; set; }
    }
}
