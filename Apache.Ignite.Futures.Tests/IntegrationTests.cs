using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Futures.Tests.TestObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.IO;

namespace Apache.Ignite.Futures.Tests
{
    [TestClass()]
    public class IntegrationTests
    {
        [TestMethod()]
        public void TestGetBeforeOperationCompletes()
        {
            var svcBuildDir = Path.Combine(Paths.OutDir, "..", "..", "..", "build");
            var svcClsPath = Paths.GetModuleClasses(svcBuildDir);

            var igniteClientCfg = new IgniteConfiguration
            {
                SpringConfigUrl = "ignite-client.xml",
                JvmClasspath = svcClsPath,
                JvmOptions = new List<string>
                {
                    "-Djava.net.preferIPv4Stack=true",
                    "-Djava.util.logging.config.file=" + Paths.LogPropsPath
                },
                BinaryConfiguration = new BinaryConfiguration()
                {
                    NameMapper = new DotnetToJavaNameMapper()
                }
            };

            using (var igniteJava = new IgniteJavaNode("ignite-server.xml", svcClsPath))
            {
                Assert.IsFalse(
                    igniteJava.IsFailed, 
                    "Ignite Java Server did not start OK. Make sure: 1) IGNITE_HOME environment variable is properly " + 
                    "set. 2) Calculator service classes are built using command 'gradle classes testClasses'.");

                using (var ignite = Ignition.Start(igniteClientCfg))
                {
                    var calc = ignite.GetServices().GetServiceProxy<ICalculator>("Calculator", true);

                    var fut = calc.sum(1, 2);

                    System.Console.WriteLine($">>> FUTURE: {fut}");
                }
            }
        }
    }
}