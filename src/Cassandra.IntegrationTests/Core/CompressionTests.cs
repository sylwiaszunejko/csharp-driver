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
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class CompressionTests : SharedClusterTest
    {
        [Test, TestTimeout(120000)]
        public void Lz4_Compression_Under_Heavy_Concurrency_Test()
        {
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .WithCompression(CompressionType.LZ4)
                                        .Build())
            {
                var session = cluster.Connect(KeyspaceName);
                const string table = "tbl_lz4_1";
                session.Execute(string.Format("CREATE TABLE {0} (id uuid PRIMARY KEY, value text)", table));
                var psInsert = session.Prepare(string.Format("INSERT INTO {0} (id, value) VALUES (?, ?)", table));
                var psSelect = session.Prepare(string.Format("SELECT * FROM {0}", table));
                var values = new[]
                {
                    //different values
                    string.Join("", Enumerable.Repeat("abc", 10)), // 3 * 10 + 16 + header + metadata = ~ 80 b on select
                    string.Join("", Enumerable.Repeat("z", 11)), // 11 + 16 + header + metadata = ~ 60
                    string.Join("", Enumerable.Repeat("hello", 50)) // ~ 300b
                };

                var insertTasks = new Task[128];
                for (var i = 0; i < insertTasks.Length; i++)
                {
                    insertTasks[i] = session.ExecuteAsync(psInsert.Bind(Guid.NewGuid(), values[i % values.Length]));
                }
                Task.WaitAll(insertTasks);
                //High concurrency level
                TestHelper.Invoke(() =>
                {
                    var tasks = new Task[20];
                    //retrieve 100 rows n times
                    for (var i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = session.ExecuteAsync(psSelect.Bind().SetPageSize(100));
                    }
                    Task.WaitAll(tasks);
                }, 10);
                //No concurrency
                TestHelper.Invoke(() => session.Execute(psSelect.Bind().SetPageSize(100)), 200);
            }
        }
    }
}