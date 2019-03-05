using Apache.Ignite.Core;
using Castle.DynamicProxy;
using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures.TopicMessage
{
    internal class ServiceInterceptor<T> : IInterceptor where T : class
    {
        private static readonly ModuleBuilder moduleBuilder = AppDomain.CurrentDomain
            .DefineDynamicAssembly(
            new AssemblyName("Apache.Ignite.Futures.TopicMessage.ServiceInterceptor.Dynamic"),
            AssemblyBuilderAccess.Run)
            .DefineDynamicModule("IgniteServiceTypes");

        private static readonly Lazy<Type> asyncJavaSvcType = new Lazy<Type>(() => CreateIgniteAsyncJavaServiceType());

        private readonly IIgnite ignite;
        private readonly string name;

        private readonly Lazy<object> asyncSvcProxy;
        private readonly Lazy<T> dfltSvcProxy;

        public ServiceInterceptor(IIgnite ignite, string name)
        {
            this.ignite = ignite;
            this.name = name;

            asyncSvcProxy = new Lazy<object>(GetAsyncServiceProxy);
            dfltSvcProxy = new Lazy<T>(() => ignite.GetServices().GetServiceProxy<T>(name, true));
        }

        public void Intercept(IInvocation invocation)
        {
            // We proxy only async method. Async methods are the methods returning a Task and having CancellationToken
            // as the last parameter.
            if (typeof(Task).IsAssignableFrom(invocation.Method.ReturnType) &&
                invocation.Arguments.Length > 0 &&
                invocation.Arguments[invocation.Arguments.Length - 1] is CancellationToken)
                CallAsyncJavaMethod(invocation);
            else
                invocation.ReturnValue = invocation.Method.Invoke(dfltSvcProxy.Value, invocation.Arguments);
        }

        /// <summary>
        /// Convert .NET-style async invocation to Java style and invoke the new method.
        /// </summary>
        private void CallAsyncJavaMethod(IInvocation invocation)
        {
            // Remove cancellation token from the Java service args
            CancellationToken cancellation = (CancellationToken)invocation.Arguments[invocation.Arguments.Length - 1];

            object[] javaSvcArgs = new object[invocation.Arguments.Length - 1];
            Array.Copy(invocation.Arguments, javaSvcArgs, invocation.Arguments.Length - 1);

            var javaMethod = asyncJavaSvcType.Value.GetTypeInfo().GetDeclaredMethod(invocation.Method.Name);

            var javaFuture = (TopicMessageFuture)javaMethod.Invoke(asyncSvcProxy.Value, javaSvcArgs);

            var futureResultType = typeof(TaskCompletionSource<>)
                .MakeGenericType(invocation.Method.ReturnType.GetGenericArguments());

            dynamic futureResult = Activator.CreateInstance(futureResultType);

            new ClientSideHandler(ignite, futureResult, cancellation, javaFuture);

            invocation.ReturnValue = futureResult.Task;
        }

        /// <summary>
        /// Convert Ignite.NET async service interface to Ignite Java async service interface according to these rules:
        /// <list type="bullet">
        /// <item>Ignite.NET async methods are the methods having <see cref="Task"/> as a return type.</item>
        /// <item>Ignite Java async methods return <see cref="TopicMessageFuture"/></item>
        /// <item>Ignite Java async methods have same arguments as Ignite.NET async methods except the 
        /// last <see cref="CancellationToken"/> argument</item>
        /// </list>
        /// </summary>
        /// <returns>Ignite Java async service interface type</returns>
        private static Type CreateIgniteAsyncJavaServiceType()
        {
            Type origType = typeof(T);

            var newTypeName = $"{origType.FullName}AsyncJava";

            var newType = moduleBuilder.GetType(newTypeName);

            if (newType != null)
                return newType;

            var igniteSvcTypeBuilder = moduleBuilder.DefineType(
                newTypeName,
                origType.Attributes,
                null);

            var origAsyncMethods = origType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Where(m => typeof(Task).IsAssignableFrom(m.ReturnType));

            foreach (var origMethod in origAsyncMethods)
            {
                var igniteMethodBuilder = igniteSvcTypeBuilder.DefineMethod(
                    origMethod.Name,
                    origMethod.Attributes);

                igniteMethodBuilder.SetReturnType(typeof(TopicMessageFuture));

                var origArgs = origMethod.GetParameters();
                Type[] igniteSvcArgs = origMethod.GetParameters()
                    .Take(origArgs.Length - 1)
                    .Select(p => p.ParameterType)
                    .ToArray();

                igniteMethodBuilder.SetParameters(igniteSvcArgs);
            }

            return igniteSvcTypeBuilder.CreateType();
        }

        /// <returns>Ignite service proxy for async Java service methods.</returns>
        private object GetAsyncServiceProxy()
        {
            var igniteSvcs = ignite.GetServices();

            var getProxyMethod = igniteSvcs.GetType().GetMethods()
                .Where(m => m.Name.Equals("GetServiceProxy"))
                .Last()
                .MakeGenericMethod(new[] { asyncJavaSvcType.Value });

            return getProxyMethod.Invoke(igniteSvcs, new object[] { name, true });
        }
    }
}
