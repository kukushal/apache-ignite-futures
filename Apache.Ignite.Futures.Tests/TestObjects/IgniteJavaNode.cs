using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    /// <summary>
    ///  Ignite Java Node.
    /// </summary>
    public sealed class IgniteJavaNode : IDisposable
    {
        private static readonly object consoleLock = new object();

        private readonly StringBuilder errors = new StringBuilder();
        private readonly StringBuilder output = new StringBuilder();
        private readonly Process process;
        private readonly EventWaitHandle srvStartedOrFailed = new AutoResetEvent(false);
        private readonly string name;

        /// <summary>
        /// Start the <see cref="IgniteJavaNode"/>.
        /// </summary>
        /// <param name="cfgPath">Path to Ignite spring XML configuration file.</param>
        /// <param name="userLibs">Optional classpath to user classes and JARs</param>
        public IgniteJavaNode(string cfgPath, string userLibs)
        {
            if (cfgPath == null)
                throw new ArgumentNullException(nameof(cfgPath));

            if (!File.Exists(cfgPath))
                throw new ArgumentException($"Ignite configuration not found: ${cfgPath}");

            name = Path.GetFileNameWithoutExtension(cfgPath);

            process = StartProcess(cfgPath, userLibs);

            const int MaxWaitTimeSec = 60;

            if (!srvStartedOrFailed.WaitOne(MaxWaitTimeSec * 1000))
            {
                errors.AppendFormat(
                    CultureInfo.CurrentCulture,
                    "Ignite Java Node did not start in {0} seconds\n",
                    MaxWaitTimeSec);
            }
        }

        /// <summary>
        /// Terminate the node.
        /// </summary>
        public void Dispose()
        {
            process.Kill();
            process.Dispose();
        }

        /// <summary>
        /// Standard output from the node.
        /// </summary>
        public string Output
        {
            get
            {
                lock (output)
                    return output.ToString();
            }
        }

        /// <summary>
        /// Error output from the node.
        /// </summary>
        public string Errors
        {
            get
            {
                lock (errors)
                    return errors.ToString();
            }
        }

        public bool IsFailed
        {
            get { return Errors.Length > 0; }
        }

        public Guid Id { get; private set; }

        private Process StartProcess(string cfgPath, string userLibs)
        {
            if (string.IsNullOrEmpty(Paths.IgniteHome))
                throw new ApplicationException("IGNITE_HOME environment variable must be set.");

            string clsPath = string.Join(Path.PathSeparator.ToString(), Paths.IgniteLibs, userLibs ?? string.Empty);

            var javaArgs = $"-cp {clsPath} -Djava.util.logging.config.file={Paths.LogPropsPath} " +
                "-Djava.net.preferIPv4Stack=true " +
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 " +
                "org.apache.ignite.startup.cmdline.CommandLineStartup " +
                cfgPath;

            var processInfo = new ProcessStartInfo("java.exe", javaArgs)
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                UseShellExecute = false,
                ErrorDialog = false
            };

            var process = new Process()
            {
                StartInfo = processInfo
            };

            // Java Util Logger logs to stderr by default. Handle both stdout and stderr with the same handler.
            process.OutputDataReceived += (_, args) => HandleOutput(args.Data);
            process.ErrorDataReceived += (sender, args) => HandleOutput(args.Data);

            process.Start();

            process.BeginErrorReadLine();
            process.BeginOutputReadLine();

            return process;
        }

        private void HandleOutput(string line)
        {
            if (line == null)
                return;

            var outputStr = string.Empty;
            lock (output)
            {
                output.AppendLine(line);
                outputStr = output.ToString();
            }

            if (outputStr.Contains("org.apache.ignite.IgniteException"))
            {
                lock (errors)
                {
                    errors.AppendLine(line);
                }

                srvStartedOrFailed.Set();
            }
            else
            {
                var idMatch = new Regex(@">>>\sLocal\snode\s\[ID=([A-Z,a-z,0-9,\-]+)\,").Match(outputStr);

                if (idMatch.Success && idMatch.Groups.Count > 1)
                    Id = new Guid(idMatch.Groups[1].Value.Replace("-", string.Empty));

                srvStartedOrFailed.Set();
            }

            lock (consoleLock)
            {
                Console.WriteLine(string.Concat(name, " | ", line));
            }
        }
    }
}
