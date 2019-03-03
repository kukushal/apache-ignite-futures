namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// <see cref="TopicMessageFuture"/> state.
    /// </summary>
    public enum State
    {
        Init,

        Active,

        Done,

        Cancelled
    }
}
