namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// <see cref="TopicMessageFuture"/> state.
    /// </summary>
    internal enum State
    {
        Init,

        Active,

        Done,

        Cancelled
    }
}
