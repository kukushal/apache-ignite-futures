using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Deployment;
using Apache.Ignite.Futures.Tests.TestObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Ignite.Futures.Tests
{
    /// <summary>
    /// .NET client calls Ignite Java service.
    /// </summary>
    [TestClass()]
    public class DotNetCallingJavaTests
    {
        [TestMethod()]
        public void GetResultBeforeOperationCompletes()
        {
            using (Ignition.Start(IgniteServerConfiguration))
            {
                using (var ignite = Ignition.Start(IgniteClientConfiguration))
                {
                    var calc = new ServiceLocator(ignite).GetService<ICalculator>("Calculator");

                    Task<int> task = calc.sum(1, 2, 2000, null, CancellationToken.None);

                    var result = task.Result;

                    Assert.AreEqual(1 + 2, result);
                }
            }
        }

        [TestMethod()]
        public void GetResultAfterOperationCompletes()
        {
            using (Ignition.Start(IgniteServerConfiguration))
            {
                using (var ignite = Ignition.Start(IgniteClientConfiguration))
                {
                    var calc = new ServiceLocator(ignite).GetService<ICalculator>("Calculator");

                    Task<int> task = calc.sum(1, 2, 10, null, CancellationToken.None);

                    Thread.Sleep(1000);

                    var result = task.Result;

                    Assert.AreEqual(1 + 2, result);
                }
            }
        }

        [TestMethod()]
        public void GetSynchronousOperationResult()
        {
            using (Ignition.Start(IgniteServerConfiguration))
            {
                using (var ignite = Ignition.Start(IgniteClientConfiguration))
                {
                    var calc = new ServiceLocator(ignite).GetService<ICalculator>("Calculator");

                    Task<int> task = calc.sum(1, 2, 0 /* 0 means sync execution */, null, CancellationToken.None);

                    var result = task.Result;

                    Assert.AreEqual(1 + 2, result);
                }
            }
        }

        [TestMethod()]
        public void GetResultFails()
        {
            using (Ignition.Start(IgniteServerConfiguration))
            {
                using (var ignite = Ignition.Start(IgniteClientConfiguration))
                {
                    const String ExpFailure = "FAILURE!";

                    var calc = new ServiceLocator(ignite).GetService<ICalculator>("Calculator");

                    Task<int> task = calc.sum(1, 2, 2000, ExpFailure, CancellationToken.None);

                    string actualFailure = null;

                    try
                    {
                        var result = task.Result;
                    }
                    catch (AggregateException ex)
                    {
                        if (ex.InnerException is ServiceException)
                            actualFailure = ex.InnerException.Message;
                    }

                    Assert.AreEqual(ExpFailure, actualFailure);
                }
            }
        }

        [TestMethod()]
        [ExpectedException(typeof(TaskCanceledException))]
        public void CancelOperationFromSameClient()
        {
            using (Ignition.Start(IgniteServerConfiguration))
            {
                using (var ignite = Ignition.Start(IgniteClientConfiguration))
                {
                    var calc = new ServiceLocator(ignite).GetService<ICalculator>("Calculator");

                    var cancelSrc = new CancellationTokenSource(TimeSpan.FromMinutes(1));

                    Task<int> task = calc.sum(1, 2, 20000, null, cancelSrc.Token);

                    cancelSrc.Cancel();

                    try
                    {
                        task.Wait(2000);
                    }
                    catch (AggregateException e)
                    {
                        Assert.IsTrue(calc.wasCancelled());

                        throw e.InnerException; // must be expected TaskCanceledException
                    }
                }
            }
        }

        private static readonly string svcBuildDir = Path.Combine(Paths.OutDir, "..", "..", "..", "build");
        private static readonly string svcClsPath = Paths.GetModuleClasses(svcBuildDir);

        private IgniteConfiguration IgniteServerConfiguration
        {
            get
            {
                return new IgniteConfiguration
                {
                    SpringConfigUrl = "ignite-service.xml",
                    JvmClasspath = svcClsPath,
                    PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain,
                    JvmOptions = new List<string>
                    {
                        "-Djava.net.preferIPv4Stack=true",
                        "-Djava.util.logging.config.file=" + Paths.LogPropsPath,
                        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
                    },
                };
            }
        }

        private IgniteConfiguration IgniteClientConfiguration
        {
            get
            {
                return new IgniteConfiguration
                {
                    SpringConfigUrl = "ignite-client.xml",
                    JvmClasspath = svcClsPath,
                    PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain,
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
            }
        }
    }
}