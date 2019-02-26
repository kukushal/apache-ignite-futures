namespace Apache.Ignite.Futures.TopicMessage
{
    /// <summary>
    /// Cancellation confirmation sent from the server to the client.
    /// </summary>
    internal class CancelAck
    {
        /// <summary>
        /// Gets or sets cancellation failure or <code>null</code> is cancellation succeeded.
        /// </summary>
        public string Failure { get; set; }
    }
}
