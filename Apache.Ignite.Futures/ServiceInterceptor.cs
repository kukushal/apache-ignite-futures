using Apache.Ignite.Core;
using Castle.DynamicProxy;

namespace Apache.Ignite.Futures
{
    class ServiceInterceptor<T> : IInterceptor where T: class
    {
        private readonly IIgnite ignite;
        private readonly string name;

        public ServiceInterceptor(IIgnite ignite, string name)
        {
            this.ignite = ignite;
            this.name = name;
        }

        public void Intercept(IInvocation invocation)
        {
            var proxy = ignite.GetServices().GetServiceProxy<T>(name, false);

            invocation.ReturnValue = invocation.Method.Invoke(proxy, invocation.Arguments);
        }
    }
}
