﻿using Apache.Ignite.Core;
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
    [TestClass()]
    public class IntegrationTests
    {
        [TestMethod()]
        public void GetResultBeforeOperationCompletes()
        {
            using (var igniteJava = new IgniteJavaNode("ignite-server.xml", svcClsPath))
            {
                using (var ignite = Ignition.Start(IgniteClientConfiguration))
                {
                    var calc = new ServiceLocator(ignite).GetService<ICalculator>("Calculator");

                    Task<int> task = calc.Sum(1, 2, 2000, CancellationToken.None);

                    var result = task.Result;

                    Assert.AreEqual(1 + 2, result);
                }
            }
        }

        [TestMethod()]
        [ExpectedException(typeof(TaskCanceledException))]
        public void CancelOperationFromSameClient()
        {
            using (var igniteJava = new IgniteJavaNode("ignite-server.xml", svcClsPath))
            {
                using (var ignite = Ignition.Start(IgniteClientConfiguration))
                {
                    var calc = new ServiceLocator(ignite).GetService<ICalculator>("Calculator");

                    var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));

                    Task<int> task = calc.Sum(1, 2, 20000, cts.Token);

                    cts.Cancel();

                    try
                    {
                        task.Wait(2000);
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException; // must be expected TaskCanceledException
                    }
                }
            }
        }

        private static readonly string svcBuildDir = Path.Combine(Paths.OutDir, "..", "..", "..", "build");
        private static readonly string svcClsPath = Paths.GetModuleClasses(svcBuildDir);

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
                        "-Djava.util.logging.config.file=" + Paths.LogPropsPath,
                        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"
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