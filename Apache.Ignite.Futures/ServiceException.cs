using System;
using System.Runtime.Serialization;

namespace Apache.Ignite.Futures
{
    /// <summary>
    /// Injector Exception
    /// </summary>
    public class ServiceException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceException"/> class.
        /// </summary>
        public ServiceException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceException"/> class with a specified error message.
        /// </summary>
        public ServiceException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceException"/> class with a specified error message 
        /// and a reference to the inner exception that is the cause of this exception.
        /// </summary>
        public ServiceException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceException"/> class with serialized data.
        /// </summary>
        protected ServiceException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
