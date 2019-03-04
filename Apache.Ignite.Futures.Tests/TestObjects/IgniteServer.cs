using Apache.Ignite.Core;
using System;
using System.Threading;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    /// <summary>
    /// Ignite.NET server node hosting <see cref="CalculatorService"/>.
    /// </summary>
    public class IgniteServer
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.StartFromApplicationConfiguration())
            {
                ignite.GetServices().DeployClusterSingleton("Calculator", new CalculatorService2());

                Console.WriteLine(">>> Ignite started OK. Press ENTER to exit...");

                Console.Read();
            }
        }
    }
}
