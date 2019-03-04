namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Result sent from the server to the client.
    /// </summary>
    public class Result
    {
        /// <summary>
        /// Gets or sets result or <code>null</code> if the operation has no result or the operation failed.
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// Gets or sets failure or <code>null</code> if the operation was successful.
        /// </summary>
        public string Failure { get; set; }
    }
}
