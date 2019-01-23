using Apache.Ignite.Core;
using Apache.Ignite.Core.Messaging;
using Castle.DynamicProxy;
using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures
{
    class ServiceInterceptor<T> : IInterceptor where T : class
    {
        private static readonly ModuleBuilder moduleBuilder = AppDomain.CurrentDomain
            .DefineDynamicAssembly(new AssemblyName("Apache.Ignite.Futures.Dynamic"), AssemblyBuilderAccess.Run)
            .DefineDynamicModule("IgniteServiceTypes");

        private readonly IIgnite ignite;
        private readonly string name;
        private readonly Type javaSvcType;

        public ServiceInterceptor(IIgnite ignite, string name)
        {
            this.ignite = ignite;
            this.name = name;
            javaSvcType = CreateIgniteServiceType();
        }

        public void Intercept(IInvocation invocation)
        {
            var javaSvcProxy = GetServiceProxy();

            // Remove cancellation token from the Java service args
            CancellationToken ct = (CancellationToken)invocation.Arguments[invocation.Arguments.Length - 1];

            object[] javaSvcArgs = new object[invocation.Arguments.Length - 1];
            Array.Copy(invocation.Arguments, javaSvcArgs, invocation.Arguments.Length - 1);

            var javaMethod = javaSvcType.GetTypeInfo().GetDeclaredMethod(ToJavaMethodName(invocation.Method.Name));

            var javaFuture = (TopicMessageFuture)javaMethod.Invoke(javaSvcProxy, javaSvcArgs);

            var igniteMsgs = ignite.GetMessaging();

            var tcsType = typeof(TaskCompletionSource<>)
                .MakeGenericType(invocation.Method.ReturnType.GetGenericArguments());
            
            dynamic tcs = Activator.CreateInstance(tcsType);

            // Send cancellation request to the server if user cancels the async operation
            ct.Register(
                () =>
                {
                    igniteMsgs.Send("CANCEL_REQ", javaFuture.Topic);
                    tcs.SetCanceled();
                });

            // Complete the task when the result is received
            igniteMsgs.LocalListen(new MessageListener(tcs), javaFuture.Topic);

            invocation.ReturnValue = tcs.Task;
        }

        private class MessageListener : IMessageListener<object>
        {
            private dynamic tcs;

            public MessageListener(dynamic tcs)
            {
                this.tcs = tcs;
            }

            public bool Invoke(Guid nodeId, object result)
            {
                tcs.SetResult(result);

                return false; // unsubscribe
            }
        }

        /// <summary>
        /// Convert Ignite.NET async service interface to Ignite Java async service interface according to these rules:
        /// <list type="bullet">
        /// <item>Ignite.NET async methods are the methods having <see cref="Task"/> as a return type.</item>
        /// <item>Ignite Java async methods return <see cref="TopicMessageFuture"/></item>
        /// <item>Ignite.NET and Ignite Java methods have same names bu different naming convention: 
        /// UpperCamelCase and lowerCamelCase correspondigly</item>
        /// <item>Ignite Java async methods have same arguments as Ignite.NET async methods except the 
        /// last <see cref="CancellationToken"/> argument</item>
        /// </list>
        /// </summary>
        /// <returns>Ignite Java async service interface type</returns>
        private static Type CreateIgniteServiceType()
        {
            Type origType = typeof(T);

            var igniteSvcTypeBuilder = moduleBuilder.DefineType(
                origType.Name,
                origType.Attributes,
                null);

            var origAsyncMethods = origType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Where(m => typeof(Task).IsAssignableFrom(m.ReturnType));

            foreach (var origMethod in origAsyncMethods)
            {
                var igniteMethodBuilder = igniteSvcTypeBuilder.DefineMethod(
                    ToJavaMethodName(origMethod.Name),
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

        private static string ToJavaMethodName(string dotNetMethodName)
        {
            return string.IsNullOrEmpty(dotNetMethodName) 
                ? dotNetMethodName
                : dotNetMethodName[0].ToString().ToLowerInvariant() + dotNetMethodName.Substring(1);
        }

        /// <returns>Ignite service proxy for the Java service type.</returns>
        private object GetServiceProxy()
        {
            var igniteSvcs = ignite.GetServices();

            var getProxyMethod = igniteSvcs.GetType().GetMethods()
                .Where(m => m.Name.Equals("GetServiceProxy"))
                .Last()
                .MakeGenericMethod(new[] { javaSvcType });

            return getProxyMethod.Invoke(igniteSvcs, new object[] { name, true });
        }
    }
}
