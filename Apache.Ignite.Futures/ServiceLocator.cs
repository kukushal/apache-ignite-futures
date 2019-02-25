using Apache.Ignite.Core;
using Apache.Ignite.Futures.TopicMessage;
using Castle.DynamicProxy;

namespace Apache.Ignite.Futures
{
    public class ServiceLocator
    {
        private readonly IIgnite ignite;

        private static ProxyGenerator Generator => new ProxyGenerator();

        public ServiceLocator(IIgnite ignite)
        {
            this.ignite = ignite;
        }

        public T GetService<T>(string name) where T : class
        {
            return Generator.CreateInterfaceProxyWithoutTarget<T>(new ServiceInterceptor<T>(ignite, name));
        }
    }
}
