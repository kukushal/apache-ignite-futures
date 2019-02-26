namespace Apache.Ignite.Futures.TopicMessage
{
    internal class TopicMessageFuture
    {
        public string Topic { get; set; }

        public State State { get; set; }

        public object Result { get; set; }

        public long CancelTimeout { get; set; }
    }
}
