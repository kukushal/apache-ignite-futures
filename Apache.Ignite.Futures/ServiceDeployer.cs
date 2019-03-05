using Apache.Ignite.Core;
using Apache.Ignite.Core.Resource;
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
        private static readonly AssemblyBuilder asmBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(
            new AssemblyName("Apache.Ignite.Futures.ServiceDeployer.Dynamic"),
            AssemblyBuilderAccess.Run);

        private static readonly ModuleBuilder moduleBuilder = asmBuilder.DefineDynamicModule("IgniteServices");

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
            var extType = CreateIgniteServiceType(service.GetType());

            return (IService)Activator.CreateInstance(extType);
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

            // Define new type inhertiting original type
            var igniteSvcTypeBuilder = moduleBuilder.DefineType(
                origType.FullName,
                origType.Attributes,
                origType);

            // Inject field "ignite" using InstanceResourceAttribute. We need ignite to implement new async methods.
            var igniteResAttr = new CustomAttributeBuilder(
                typeof(InstanceResourceAttribute).GetConstructor(new Type[0]),
                new object[0]);

            var igniteFldBuilder = igniteSvcTypeBuilder.DefineField("ignite", typeof(IIgnite), FieldAttributes.Private);
            igniteFldBuilder.SetCustomAttribute(igniteResAttr);

            // Async methods are the ones returning a Task
            var origAsyncMethods = origType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Where(m => typeof(Task).IsAssignableFrom(m.ReturnType));

            foreach (var origMethod in origAsyncMethods)
            {
                
                // DEFINITION of the Java async method counterpart
                // 1. Remove the last CancellationToken parameter:
                var origArgs = origMethod.GetParameters();
                var igniteSvcArgs = origMethod.GetParameters().Take(origArgs.Length - 1).ToArray();
                var igniteSvcArgTypes = igniteSvcArgs.Select(p => p.ParameterType).ToArray();

                // 2. Return a TopicMessageFuture
                var javaMethodBuilder = igniteSvcTypeBuilder.DefineMethod(
                    origMethod.Name,
                    origMethod.Attributes,
                    typeof(TopicMessageFuture),
                    igniteSvcArgTypes);

                // IMPLEMENTATION of the Java async method counterpart
                var javaMethodIL = javaMethodBuilder.GetILGenerator();

                // 1. Types and locals:
                var genericArgs = origMethod.ReturnType.GetGenericArguments();
                var taskType = typeof(Task<>).MakeGenericType(genericArgs);
                var srvHdlrType = typeof(ServerSideHandler<>).MakeGenericType(genericArgs);

                javaMethodIL.DeclareLocal(typeof(CancellationTokenSource)); // index 0
                javaMethodIL.DeclareLocal(taskType); // index 1
                javaMethodIL.DeclareLocal(srvHdlrType); // index 2
                javaMethodIL.DeclareLocal(typeof(TopicMessageFuture)); // index 3

                // 2. Create CancellationTokenSource using default constructor
                javaMethodIL.Emit(OpCodes.Newobj, typeof(CancellationTokenSource).GetConstructor(new Type[0]));
                javaMethodIL.Emit(OpCodes.Stloc_0); // store as a local var at index 0

                // 3. Call original .NET version of the method by appending the cancellation token parameter.
                var tokenGetter = typeof(CancellationTokenSource)
                    .GetProperty(nameof(CancellationTokenSource.Token))
                    .GetGetMethod();

                for (var i = 0; i <= igniteSvcArgs.Length; i++)
                    javaMethodIL.Emit(OpCodes.Ldarg_S, i); // Load "this" and all args

                javaMethodIL.Emit(OpCodes.Ldloc_0); // the CancellationTokenSource is at index 0
                javaMethodIL.Emit(OpCodes.Callvirt, tokenGetter); // get CancellationToken and push it onto the stack

                javaMethodIL.Emit(OpCodes.Call, origMethod); // call the original async method
                javaMethodIL.Emit(OpCodes.Stloc_1); // store the returned Task locally at index 1 

                // 4. Create ServerSideHandler<T>(ignite, task, cancellationToken) for the .NET method's Task
                javaMethodIL.Emit(OpCodes.Ldarg_0); // "this"
                javaMethodIL.Emit(OpCodes.Ldfld, igniteFldBuilder); // injected Ignite resource
                javaMethodIL.Emit(OpCodes.Ldloc_1); // task
                javaMethodIL.Emit(OpCodes.Ldloc_0); // cancellation token
                javaMethodIL.Emit(OpCodes.Newobj, srvHdlrType.GetConstructors()[0]); // create server side handler
                javaMethodIL.Emit(OpCodes.Stloc_2); // store server side handler locally at index 2

                // 5. Return ServiceSideHander's future
                var futureGetter = srvHdlrType
                    .GetProperty(nameof(ServerSideHandler<object>.Future))
                    .GetGetMethod();

                javaMethodIL.Emit(OpCodes.Ldloc_2); // server side handler
                javaMethodIL.Emit(OpCodes.Callvirt, futureGetter); // get Future and push it onto the stack
                javaMethodIL.Emit(OpCodes.Ret);
            }

            return igniteSvcTypeBuilder.CreateType();
        }
    }
}
