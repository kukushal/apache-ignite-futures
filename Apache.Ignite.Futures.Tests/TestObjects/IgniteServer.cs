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
            using (IIgnite ignite = Ignition.StartFromApplicationConfiguration())
            {
                ignite.GetServices().DeployClusterSingleton("Calculator", new CalculatorService());

                Console.WriteLine(">>> Ignite started OK. Press any key to exit...");

                var k = Console.ReadKey();

                Console.WriteLine($">>> {k.KeyChar} pressed. Exiting...");
            }
        }
    }
}
