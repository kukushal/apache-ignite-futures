using Apache.Ignite.Core;
using Apache.Ignite.Futures.TopicMessage;
using Castle.DynamicProxy;
using System;

namespace Apache.Ignite.Futures
{
    /// <summary>
    /// Use <see cref="ServiceLocator"/> to obtain .NET staticaly typed proxies to Ignite Java services.
    /// </summary>
    public class ServiceLocator
    {
        private readonly IIgnite ignite;

        private static ProxyGenerator Generator => new ProxyGenerator();

        /// <summary>
        /// Constructor
        /// </summary>
        public ServiceLocator(IIgnite ignite)
        {
            this.ignite = ignite ?? throw new ArgumentNullException(nameof(ignite));
        }

        /// <returns>.NET staticaly typed proxy to Ignite Java service.</returns>
        public T GetService<T>(string name) where T : class
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return Generator.CreateInterfaceProxyWithoutTarget<T>(new ServiceInterceptor<T>(ignite, name));
        }
    }
}
