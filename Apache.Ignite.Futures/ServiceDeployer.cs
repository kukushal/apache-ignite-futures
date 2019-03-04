using Apache.Ignite.Core;
using Apache.Ignite.Core.Services;
using Apache.Ignite.Futures.TopicMessage;
using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures
{
    /// <summary>
    /// Use <see cref="ServiceDeployer"/> to deploy Ignite.NET services available to both Ignite.NET and Ignite Java
    /// clients.
    /// </summary>
    public class ServiceDeployer
    {
        private static readonly ModuleBuilder moduleBuilder = AppDomain.CurrentDomain
            .DefineDynamicAssembly(
            new AssemblyName("Apache.Ignite.Futures.ServiceDeployer.Dynamic"),
            AssemblyBuilderAccess.Run)
            .DefineDynamicModule("IgniteServiceTypes");

        private readonly IIgnite ignite;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ServiceDeployer(IIgnite ignite)
        {
            this.ignite = ignite ?? throw new ArgumentNullException(nameof(ignite));
        }

        /// <summary>
        /// Deploy Ignite.NET service available to both Ignite.NET and Ignite Java clients.
        /// </summary>
        /// <param name="name">Service name.</param>
        /// <param name="service">Service implementation.</param>
        public void Deploy(string name, IService service)
        {
            ignite.GetServices().DeployClusterSingleton(name, ExtendForJava(service));
        }

        /// <summary>
        /// Enhance Ignite.NET asynchronous service to be consumed by Ignite Java clients.
        /// </summary>
        private IService ExtendForJava(IService service)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Generate a new <see cref="Type"/> by inheriting Ignite.NET async service implementation and adding 
        /// Java async methods counterparts according to these rules:
        /// <list type="bullet">
        /// <item>Ignite.NET async methods are the methods having <see cref="Task"/> as a return type.</item>
        /// <item>Ignite Java async methods return <see cref="TopicMessageFuture"/></item>
        /// <item>Ignite Java async methods have same arguments as Ignite.NET async methods except the 
        /// last <see cref="CancellationToken"/> argument</item>
        /// </list>
        /// </summary>
        /// <returns>Ignite Java async service interface type</returns>
        private static Type CreateIgniteServiceType(Type origType)
        {
            var newType = moduleBuilder.GetType(origType.Name);

            if (newType != null)
                return newType;

            var igniteSvcTypeBuilder = moduleBuilder.DefineType(
                origType.Name,
                origType.Attributes,
                origType);

            // Async methods are the ones returning a Task
            var origAsyncMethods = origType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Where(m => typeof(Task).IsAssignableFrom(m.ReturnType));

            foreach (var origMethod in origAsyncMethods)
            {
                // DEFINITION of the Java async method counterpart
                // 1. Remove the last CancellationToken parameter:
                var origArgs = origMethod.GetParameters();
                var igniteSvcArgs = origMethod.GetParameters().Take(origArgs.Length - 1);
                var igniteSvcArgTypes = igniteSvcArgs.Select(p => p.ParameterType).ToArray();

                // 2. Return a TopicMessageFuture
                var javaMethodBuilder = igniteSvcTypeBuilder.DefineMethod(
                    origMethod.Name,
                    origMethod.Attributes,
                    typeof(TopicMessageFuture),
                    igniteSvcArgTypes);

                // IMPLEMENTATION of the Java async method counterpart
                var javaMethodIL = javaMethodBuilder.GetILGenerator();

                // 1. Create CancellationTokenSource using default constructor and store the created instance in the 
                // local variables list at index 0.
                javaMethodIL.Emit(OpCodes.Newobj, typeof(CancellationTokenSource).GetConstructors()[0]);
                javaMethodIL.Emit(OpCodes.Stloc_0);

                // 2. Call original .NET version of the method by appending the cancellation token parameter.
                foreach (var a in igniteSvcArgs)
                    javaMethodIL.Emit(OpCodes.Ldarg_S, a.Name);

                javaMethodIL.Emit(OpCodes.Ldloc_0); // the CancellationTokenSource is at index 0


                // 3. Create ServiceSideHander for the .NET method's Task

                // 4. Return ServiceSideHander's future
            }

            return igniteSvcTypeBuilder.CreateType();
        }
    }
}
