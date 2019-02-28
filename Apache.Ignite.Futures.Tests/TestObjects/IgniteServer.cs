using Apache.Ignite.Core;
using System;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    /// <summary>
    /// Ignite.NET server node hosting <see cref="CalculatorService"/>.
    /// </summary>
    public class IgniteServer
    {
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                ignite.GetServices().DeployClusterSingleton("Calculator", new CalculatorService());

                Console.WriteLine(">>> Ignite started OK. Press any key to exit...");
                Console.ReadKey();
            }
        }
    }
}
