//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    /// <summary>
    /// Test Helper class for keeping track of multiple CCM (Cassandra Cluster Manager) instances
    /// </summary>
    public static class TestClusterManager
    {
        public const string DefaultKeyspaceName = "test_cluster_keyspace";
        private static ICcmProcessExecuter _executor;
        private static int _idPrefixCounter = 0;
        private static string GetUniqueIdPrefix()
        {
            return (_idPrefixCounter++).ToString();
        }

        private static readonly Version Version2Dot0 = new Version(2, 0);
        private static readonly Version Version2Dot1 = new Version(2, 1);
        private static readonly Version Version3Dot0 = new Version(3, 0);
        private static readonly Version Version3Dot11 = new Version(3, 11);
        private static readonly Version Version4Dot0 = new Version(4, 0);
        private static readonly Version Version4Dot7 = new Version(4, 7);
        private static readonly Version Version5Dot0 = new Version(5, 0);
        private static readonly Version Version5Dot1 = new Version(5, 1);
        private static readonly Version Version6Dot0 = new Version(6, 0);
        private static readonly Version Version6Dot9 = new Version(6, 9);
        private static readonly Version Version6Dot10 = new Version(6, 10);

        /// <summary>
        /// Gets the Cassandra version used for this test run
        /// </summary>
        public static Version CassandraVersion
        {
            get
            {
                if (IsHcd)
                {
                    return Version4Dot0;
                }
                return new Version(TestClusterManager.CassandraVersionString.Split('-')[0]);
            }
        }

        private static Version _scyllaVersion;
        private static Mutex _scyllaVersionLock = new Mutex();

        public static Version ScyllaVersion
        {
            get
            {
                if (_scyllaVersion != null)
                {
                    return _scyllaVersion;
                }
                _scyllaVersionLock.WaitOne();
                try
                {
                    if (_scyllaVersion != null)
                    {
                        return _scyllaVersion;
                    }
                    var scyllaVersion = ScyllaVersionString;
                    if (scyllaVersion == null)
                    {
                        throw new TestInfrastructureException("SCYLLA_VERSION is not set");
                    }

                    if (scyllaVersion.StartsWith("release:"))
                    {
                        scyllaVersion = scyllaVersion.Substring(8);
                        if (scyllaVersion.Count(c => c == '.') == 2)
                        {
                            _scyllaVersion = new Version(scyllaVersion);
                            return _scyllaVersion;
                        }
                    }

                    TryRemove();
                    var testCluster = new CcmCluster(
                        "get-scylla-version-" + TestUtils.GetTestClusterNameBasedOnRandomString(),
                        GetUniqueIdPrefix(),
                        Executor,
                        "",
                        ScyllaVersionString);
                    testCluster.Create(1, null);
                    string versionString = testCluster.GetVersion(1);
                    _scyllaVersion = new Version(versionString);
                    return _scyllaVersion;
                }
                finally
                {
                    _scyllaVersionLock.ReleaseMutex();
                }
            }
        }

        /// <summary>
        /// Gets the IP prefix
        /// </summary>
        public static string IpPrefix
        {
            get { return "127.0.0."; }
        }

        public enum BackendType
        {
            Hcd,
            Cassandra,
            Scylla
        }

        /// <summary>
        /// "hcd", or "cassandra" (default), or "scylla", based on CCM_DISTRIBUTION
        /// </summary>
        public static BackendType CurrentBackendType
        {
            get
            {
                string distribution = Environment.GetEnvironmentVariable("CCM_DISTRIBUTION") ?? "cassandra";
                switch (distribution)
                {
                    case "hcd":
                        return BackendType.Hcd;
                    case "cassandra":
                        return BackendType.Cassandra;
                    case "scylla":
                        return BackendType.Scylla;
                    default:
                        throw new TestInfrastructureException("Unknown CCM_DISTRIBUTION value: " + distribution);
                }
            }
        }

        public static string InitialContactPoint
        {
            get { return IpPrefix + "1"; }
        }

        /// <summary>
        /// Use SCYLLA_VERSION if it's set, otherwise use CASSANDRA_VERSION
        /// </summary>
        public static string CassandraVersionString
        {
            get
            {
                if (Environment.GetEnvironmentVariable("SCYLLA_VERSION") != null)
                {
                    return "3.10.0";
                }
                return Environment.GetEnvironmentVariable("CASSANDRA_VERSION") ?? "3.11.2";
            }
        }

        public static string ScyllaVersionString
        {
            get
            {
                if (Environment.GetEnvironmentVariable("SCYLLA_VERSION") == null)
                {
                    throw new TestInfrastructureException("SCYLLA_VERSION is not set");
                }
                return Environment.GetEnvironmentVariable("SCYLLA_VERSION");
            }
        }

        public static bool IsScylla
        {
            get { return CurrentBackendType == BackendType.Scylla; }
        }

        public static bool IsHcd
        {
            get { return CurrentBackendType == BackendType.Hcd; }
        }

        public static bool CcmUseWsl => bool.Parse(Environment.GetEnvironmentVariable("CCM_USE_WSL") ?? "false");

        public static string CcmWslDistroName => Environment.GetEnvironmentVariable("CCM_WSL_DISTRO_NAME") ?? "";

        public static bool ShouldEnableBetaProtocolVersion()
        {
            return false;
        }

        public static bool SupportsDecommissionForcefully()
        {
            return TestClusterManager.CheckCassandraVersion(true, new Version(4, 0), Comparison.GreaterThanOrEqualsTo);
        }

        public static bool CheckCassandraVersion(bool requiresOss, Version version, Comparison comparison)
        {
            if (requiresOss && TestClusterManager.CurrentBackendType != BackendType.Cassandra)
            {
                return false;
            }

            var runningVersion = TestClusterManager.CassandraVersion;
            var expectedVersion = version;
            return TestCassandraVersion.VersionMatch(expectedVersion, runningVersion, comparison);
        }

        /// <summary>
        /// Get the ccm executor instance (local or remote)
        /// </summary>
        public static ICcmProcessExecuter Executor
        {
            get
            {
                if (TestClusterManager._executor != null)
                {
                    return TestClusterManager._executor;
                }

                if (TestClusterManager.CcmUseWsl)
                {
                    TestClusterManager._executor = WslCcmProcessExecuter.Instance;
                }
                else
                {
                    TestClusterManager._executor = LocalCcmProcessExecuter.Instance;
                }

                return TestClusterManager._executor;
            }
        }

        private static ITestCluster CreateNewNoRetry(int nodeLength, TestClusterOptions options, bool startCluster)
        {
            TryRemove();
            options = options ?? new TestClusterOptions();
            var testCluster = new CcmCluster(
                TestUtils.GetTestClusterNameBasedOnRandomString(),
                GetUniqueIdPrefix(),
                Executor,
                DefaultKeyspaceName,
                IsScylla ? ScyllaVersionString : CassandraVersionString);
            testCluster.Create(nodeLength, options);
            if (startCluster)
            {
                if (options.UseVNodes)
                {
                    // workaround for https://issues.apache.org/jira/browse/CASSANDRA-16364
                    foreach (var i in Enumerable.Range(1, nodeLength))
                    {
                        testCluster.Start(i, null, null, options.JvmArgs);
                    }
                }
                else
                {
                    testCluster.Start(options.JvmArgs);
                }
            }
            return testCluster;
        }

        /// <summary>
        /// Creates a new test cluster
        /// </summary>
        public static ITestCluster CreateNew(int nodeLength = 1, TestClusterOptions options = null, bool startCluster = true)
        {
            const int maxAttempts = 2;
            var attemptsSoFar = 0;
            while (true)
            {
                try
                {
                    return CreateNewNoRetry(nodeLength, options, startCluster);
                }
                catch (TestInfrastructureException ex)
                {
                    attemptsSoFar++;
                    if (attemptsSoFar >= maxAttempts)
                    {
                        throw;
                    }

                    Trace.WriteLine("Exception during ccm create / start. Retrying." + Environment.NewLine + ex.ToString());
                }
            }
        }

        /// <summary>
        /// Deprecated, use <see cref="TestClusterManager.CreateNew"/> method instead
        /// </summary>
        public static ITestCluster GetNonShareableTestCluster(int dc1NodeCount, int dc2NodeCount, int maxTries = 1, bool startCluster = true, bool initClient = true)
        {
            return GetTestCluster(dc1NodeCount, dc2NodeCount, false, maxTries, startCluster, initClient);
        }

        /// <summary>
        /// Deprecated, use <see cref="TestClusterManager.CreateNew"/> method instead
        /// </summary>
        public static ITestCluster GetNonShareableTestCluster(int dc1NodeCount, int maxTries = 1, bool startCluster = true, bool initClient = true)
        {
            if (startCluster == false)
                initClient = false;

            return GetTestCluster(dc1NodeCount, 0, false, maxTries, startCluster, initClient);
        }

        /// <summary>
        /// Deprecated, use <see cref="TestClusterManager.CreateNew"/> method instead
        /// </summary>
        public static ITestCluster GetTestCluster(int dc1NodeCount, int dc2NodeCount, bool shareable = true, int maxTries = 1, bool startCluster = true, bool initClient = true, int currentRetryCount = 0, string[] jvmArgs = null, bool useSsl = false)
        {
            var testCluster = CreateNew(
                dc1NodeCount,
                new TestClusterOptions
                {
                    Dc2NodeLength = dc2NodeCount,
                    UseSsl = useSsl,
                    JvmArgs = jvmArgs
                },
                startCluster);
            if (initClient)
            {
                testCluster.InitClient();
            }
            return testCluster;
        }

        /// <summary>
        /// Deprecated, use <see cref="TestClusterManager.CreateNew"/> method instead
        /// </summary>
        public static ITestCluster GetTestCluster(int dc1NodeCount, int maxTries = 1, bool startCluster = true, bool initClient = true)
        {
            return GetTestCluster(dc1NodeCount, 0, true, maxTries, startCluster, initClient);
        }

        /// <summary>
        /// Removes the current ccm cluster, without throwing exceptions if it fails
        /// </summary>
        public static void TryRemove()
        {
            try
            {
                Executor.ExecuteCcm("remove");
            }
            catch (Exception ex)
            {
                if (Diagnostics.CassandraTraceSwitch.Level == TraceLevel.Verbose)
                {
                    Trace.TraceError("ccm test cluster could not be removed: {0}", ex);
                }
            }
        }
    }
}
