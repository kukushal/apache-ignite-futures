namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Result sent from the server to the client.
    /// </summary>
    internal class Result
    {
        /// <summary>
        /// Gets or sets result or <code>null</code> if the operation has no result.
        /// </summary>
        public object Value { get; set; }
    }
}
