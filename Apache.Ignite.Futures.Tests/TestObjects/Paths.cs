using System;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Apache.Ignite.Futures.Tests.TestObjects
{
    public sealed class Paths
    {
        private static string igniteLibs;

        /// <summary>
        /// Prevent instantiation.
        /// </summary>
        private Paths() { }

        /// <summary>
        /// Ignite installation directory.
        /// </summary>
        public static string IgniteHome => Environment.GetEnvironmentVariable("IGNITE_HOME");

        /// <summary>
        /// This project output directory.
        /// </summary>
        public static string OutDir => Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

        /// <summary>
        /// java.util.logging properties path 
        /// </summary>
        public static string LogPropsPath => Path.Combine(OutDir, "java.util.logging.properties");

        /// <summary>
        /// Ignite libs Java class path.
        /// If IGNITE_HOME environment variable is set then put all JARs of the mandatory ignite modules on the class 
        /// path. Otherwise use JARs from the "libs" directory in the project output directory.
        /// </summary>
        public static string IgniteLibs
        {
            get
            {
                if (igniteLibs == null)
                {
                    if (IgniteHome == null)
                        igniteLibs = Path.Combine(OutDir, "libs", "*");
                    else
                    {
                        var libsDir = Path.Combine(IgniteHome, "libs");

                        bool IsIgniteModule(string dir) =>
                            new[] { "OPTIONAL", "LICENSES" }
                            .All(exclusion => !dir.ToUpperInvariant().Contains(exclusion));

                        var clsPaths = new[] { libsDir }
                            .Union(Directory.GetDirectories(libsDir))
                            .Where(dir => IsIgniteModule(dir))
                            .Select(cp => Path.Combine(cp, "*"))
                            .ToArray();

                        igniteLibs = string.Join(Path.PathSeparator.ToString(), clsPaths);
                    }
                }

                return igniteLibs;
            }
        }

        /// <summary>
        /// Get classpath of the module with the specified build (output) directory.
        /// </summary>
        public static string GetModuleClasses(string buildDir)
        {
            if (!Directory.Exists(buildDir))
                throw new ArgumentException($"Directory does not exist: {buildDir}", nameof(buildDir));

            var clsDir = Resolve(buildDir, "classes");

            if (clsDir == null)
                throw new ArgumentException(
                    $"Directory 'classes' does not exist inside build directory: {buildDir}", nameof(buildDir));

            // Assume default Maven structure: classes -> language -> source set -> ...

            var clsPaths = Directory.GetDirectories(clsDir).SelectMany(Directory.GetDirectories).ToArray();

            return string.Join(Path.PathSeparator.ToString(), clsPaths);
        }

        /// <summary>
        /// Find file or directory with specified name starting from the specified root dir.
        /// </summary>
        /// <returns>Absolute path to the found file or directory or <code>null</code> if nothing was found.</returns>
        public static string Resolve(string rootDir, string name)
        {
            if (!Directory.Exists(rootDir))
                throw new ArgumentException($"Directory does not exist: {rootDir}", nameof(rootDir));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (Path.GetFileName(rootDir) == name)
                return rootDir;

            foreach (string p in Directory.EnumerateFileSystemEntries(rootDir))
            {
                var res = (File.GetAttributes(p) & FileAttributes.Directory) == FileAttributes.Directory ?
                    Resolve(p, name) :
                    p;

                if (!string.IsNullOrEmpty(res) && Path.GetFileName(res) == name)
                    return res;
            }

            return null;
        }
    }
}
