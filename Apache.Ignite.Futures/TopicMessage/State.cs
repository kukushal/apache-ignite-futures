namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// <see cref="TopicMessageFuture"/> state.
    /// </summary>
    public enum State
    {
        /// <summary>
        /// The instance of <see cref="TopicMessageFuture"/> is created on the server and not yet sent to the client. 
        /// This state is applicable to the server-side only, while the other states are applicable to both the 
        /// client and server sides.
        /// </summary>
        Init,

        /// <summary>
        /// The operation that this <see cref="TopicMessageFuture"/> is tracking is in progress.
        /// </summary>
        Active,

        /// <summary>
        /// The operation this <see cref="TopicMessageFuture"/> is tracking is complete.
        /// </summary>
        Done,

        /// <summary>
        /// The operation this <see cref="TopicMessageFuture"/> is tracking is cancelled.
        /// </summary>
        Cancelled,

        /// <summary>
        /// The operation this <see cref="TopicMessageFuture"/> is tracking is failed.
        /// </summary>
        Failed
    }
}
