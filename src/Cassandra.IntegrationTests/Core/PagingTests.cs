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
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    /// <summary>
    /// Validates that the Session.GetRequest (called within ExecuteAsync) method uses the paging size under different scenarios
    /// </summary>
    [Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class PagingTests : SharedClusterTest
    {
        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            CreateSimpleTableAndInsert(300, "tbl_parallel_paging_read");
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Should_NotUseDefaultPageSize_When_SetOnClusterBulder()
        {
            var pageSize = 10;
            var queryOptions = new QueryOptions().SetPageSize(pageSize);
            var builder = ClusterBuilder().WithQueryOptions(queryOptions).WithDefaultKeyspace(KeyspaceName);
            builder.AddContactPoint(TestCluster.InitialContactPoint);

            const int totalRowLength = 1003;
            using (var session = builder.Build().Connect())
            {
                var tableNameAndStaticKeyVal = CreateTableWithCompositeIndexAndInsert(session, totalRowLength);
                var statementToBeBound = $"SELECT * from {tableNameAndStaticKeyVal.Item1} where label=?";
                var preparedStatementWithoutPaging = session.Prepare(statementToBeBound);
                var preparedStatementWithPaging = session.Prepare(statementToBeBound);
                var boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(tableNameAndStaticKeyVal.Item2);
                var boundStatemetWithPaging = preparedStatementWithPaging.Bind(tableNameAndStaticKeyVal.Item2);

                var rsWithSessionPagingInherited = session.ExecuteAsync(boundStatemetWithPaging).Result;

                var rsWithoutPagingInherited = Session.Execute(boundStatemetWithoutPaging);

                //Check that the internal list of items count is pageSize
                Assert.AreEqual(pageSize, rsWithSessionPagingInherited.InnerQueueCount);
                Assert.AreEqual(totalRowLength, rsWithoutPagingInherited.InnerQueueCount);

                var allTheRowsPaged = rsWithSessionPagingInherited.ToList();
                Assert.AreEqual(totalRowLength, allTheRowsPaged.Count);
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Should_PagingOnBoundStatement_When_ReceivedNumberOfRowsIsHigherThanPageSize()
        {
            var pageSize = 10;
            var totalRowLength = 1003;
            var tableNameAndStaticKeyVal = CreateTableWithCompositeIndexAndInsert(Session, totalRowLength);
            var statementToBeBound = $"SELECT * from {tableNameAndStaticKeyVal.Item1} where label=?";
            var preparedStatementWithoutPaging = Session.Prepare(statementToBeBound);
            var preparedStatementWithPaging = Session.Prepare(statementToBeBound);
            var boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(tableNameAndStaticKeyVal.Item2);
            var boundStatemetWithPaging = preparedStatementWithPaging.Bind(tableNameAndStaticKeyVal.Item2);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = Session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = Session.Execute(boundStatemetWithoutPaging);

            //Check that the internal list of items count is pageSize
            Assert.AreEqual(pageSize, rsWithPaging.InnerQueueCount);
            Assert.AreEqual(totalRowLength, rsWithoutPaging.InnerQueueCount);

            var allTheRowsPaged = rsWithPaging.ToList();
            Assert.AreEqual(totalRowLength, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Should_PagingOnBoundStatement_When_ReceivedNumberOfRowsIsOne()
        {
            var pageSize = 10;
            var totalRowLength = 11;
            var tableName = CreateSimpleTableAndInsert(totalRowLength);

            // insert a guid that we'll keep track of
            var guid = Guid.NewGuid();
            Session.Execute(string.Format("INSERT INTO {2} (id, label) VALUES({0},'{1}')", guid, "LABEL_12345", tableName));

            var statementToBeBound = "SELECT * from " + tableName + " where id=?";
            var preparedStatementWithoutPaging = Session.Prepare(statementToBeBound);
            var preparedStatementWithPaging = Session.Prepare(statementToBeBound);
            var boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(guid);
            var boundStatemetWithPaging = preparedStatementWithPaging.Bind(guid);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = Session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = Session.Execute(boundStatemetWithoutPaging);

            //Check that the internal list of items count is pageSize
            Assert.AreEqual(1, rsWithPaging.InnerQueueCount);
            Assert.AreEqual(1, rsWithoutPaging.InnerQueueCount);

            var allTheRowsPaged = rsWithPaging.ToList();
            Assert.AreEqual(1, allTheRowsPaged.Count);
        }

        [Test]
        [TestBothServersVersion(4, 0, 6, 0)]
        public void Should_PagingOnBoundStatement_When_NewResultMetadataIsSet()
        {
            if (Session.Cluster.Metadata.ControlConnection.Serializer.CurrentProtocolVersion < ProtocolVersion.V5)
            {
                Assert.Ignore("This test requires protocol v5+");
                return;
            }

            var pageSize = 10;
            var totalRowLength = 25;
            var tableName = CreateSimpleTableAndInsert(totalRowLength);

            var statementToBeBound = "SELECT * from " + tableName;
            var ps = Session.Prepare(statementToBeBound);

            var allRows = Session.Execute(ps.Bind()).ToList();
            var previousResultMetadata = ps.ResultMetadata;
            Assert.AreEqual(totalRowLength, allRows.Count);
            Assert.IsTrue(allRows.All(r => !r.ContainsColumn("new_column")));
            Assert.AreEqual(2, previousResultMetadata.RowSetMetadata.Columns.Length);

            var boundStatementManualPaging = ps.Bind().SetPageSize(pageSize).SetAutoPage(false);
            var rs = Session.Execute(boundStatementManualPaging);
            var firstPage = rs.ToList();

            Session.Execute($"ALTER TABLE {tableName} ADD (new_column text)");
            Assert.AreSame(previousResultMetadata, ps.ResultMetadata);
            Assert.AreEqual(previousResultMetadata.ResultMetadataId, ps.ResultMetadata.ResultMetadataId);
            Assert.AreEqual(2, ps.ResultMetadata.RowSetMetadata.Columns.Length);

            rs = Session.Execute(boundStatementManualPaging.SetPagingState(rs.PagingState));
            var secondPage = rs.ToList();
            Assert.AreNotSame(previousResultMetadata, ps.ResultMetadata);
            Assert.AreNotEqual(previousResultMetadata.ResultMetadataId, ps.ResultMetadata.ResultMetadataId);
            Assert.AreEqual(3, ps.ResultMetadata.RowSetMetadata.Columns.Length);

            rs = Session.Execute(boundStatementManualPaging.SetPagingState(rs.PagingState));
            var thirdPage = rs.ToList();

            var allRowsAfterAlter = Session.Execute(ps.Bind()).ToList();
            Assert.AreEqual(totalRowLength, allRowsAfterAlter.Count);

            Assert.AreEqual(pageSize, firstPage.Count);
            Assert.AreEqual(pageSize, secondPage.Count);
            Assert.AreEqual(totalRowLength - (pageSize * 2), thirdPage.Count);

            Assert.IsTrue(firstPage.All(r => !r.ContainsColumn("new_column")));
            Assert.IsTrue(secondPage.All(r => r.ContainsColumn("new_column") && r.GetValue<string>("new_column") == null));
            Assert.IsTrue(allRowsAfterAlter.All(r => r.ContainsColumn("new_column") && r.GetValue<string>("new_column") == null));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Should_PagingOnBoundStatement_When_ReceivedNumberOfRowsIsZero()
        {
            var pageSize = 10;
            var totalRowLength = 11;
            var tableName = CreateSimpleTableAndInsert(totalRowLength);

            // insert a guid that we'll keep track of
            var guid = Guid.NewGuid();

            var statementToBeBound = $"SELECT * from {tableName} where id=?";
            var preparedStatementWithoutPaging = Session.Prepare(statementToBeBound);
            var preparedStatementWithPaging = Session.Prepare(statementToBeBound);
            var boundStatemetWithoutPaging = preparedStatementWithoutPaging.Bind(guid);
            var boundStatemetWithPaging = preparedStatementWithPaging.Bind(guid);

            boundStatemetWithPaging.SetPageSize(pageSize);

            var rsWithPaging = Session.Execute(boundStatemetWithPaging);
            var rsWithoutPaging = Session.Execute(boundStatemetWithoutPaging);

            //Check that the internal list of items count is pageSize
            Assert.AreEqual(0, rsWithPaging.InnerQueueCount);
            Assert.AreEqual(0, rsWithoutPaging.InnerQueueCount);

            var allTheRowsPaged = rsWithPaging.ToList();
            Assert.AreEqual(0, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Should_PagingOnSimpleStatement_When_ReceivedNumberOfRowsIsHigherThanPageSize()
        {
            var pageSize = 10;
            var totalRowLength = 1003;
            var table = CreateSimpleTableAndInsert(totalRowLength);
            var statementWithPaging = new SimpleStatement($"SELECT * FROM {table}");
            var statementWithoutPaging = new SimpleStatement($"SELECT * FROM {table}");
            statementWithoutPaging.SetPageSize(int.MaxValue);
            statementWithPaging.SetPageSize(pageSize);

            var rsWithPaging = Session.Execute(statementWithPaging);
            var rsWithoutPaging = Session.Execute(statementWithoutPaging);

            //Check that the internal list of items count is pageSize
            Assert.True(rsWithPaging.InnerQueueCount == pageSize);
            Assert.True(rsWithoutPaging.InnerQueueCount == totalRowLength);

            var allTheRowsPaged = rsWithPaging.ToList();
            Assert.True(allTheRowsPaged.Count == totalRowLength);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Should_PagingOnQuery_When_ReceivedNumberOfRowsIsHigherThanPageSize()
        {
            var pageSize = 10;
            var totalRowLength = 1003;
            var table = CreateSimpleTableAndInsert(totalRowLength);
            var rsWithoutPaging = Session.Execute($"SELECT * FROM {table}", int.MaxValue);
            //It should have all the rows already in the inner list
            Assert.AreEqual(totalRowLength, rsWithoutPaging.InnerQueueCount);

            var rs = Session.Execute($"SELECT * FROM {table}", pageSize);
            //Check that the internal list of items count is pageSize
            Assert.AreEqual(pageSize, rs.InnerQueueCount);

            //Use Linq to iterate through all the rows
            var allTheRowsPaged = rs.ToList();

            Assert.AreEqual(totalRowLength, allTheRowsPaged.Count);
        }

        [Test]
        [TestCassandraVersion(2, 0), Repeat(10)]
        public void Should_IteratePaging_When_ParallelClientsReadRowSet()
        {
            const int pageSize = 25;
            const int totalRowLength = 300;
            const string table = "tbl_parallel_paging_read";
            var query = new SimpleStatement($"SELECT * FROM {table} LIMIT 10000").SetPageSize(pageSize);
            var rs = Session.Execute(query);
            Assert.AreEqual(pageSize, rs.GetAvailableWithoutFetching());

            var counter = 0;

            void Iterate()
            {
                Interlocked.Add(ref counter, rs.Count());
            }

            //Iterate in parallel the RowSet
            Parallel.Invoke(Iterate, Iterate, Iterate, Iterate);

            //Check that the sum of all rows in different threads is the same as total rows
            Assert.AreEqual(totalRowLength, Volatile.Read(ref counter));
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Should_IteratePaging_When_SerialReadRowSet()
        {
            var pageSize = 25;
            var totalRowLength = 300;
            var times = 10;
            var table = CreateSimpleTableAndInsert(totalRowLength);

            var statement = new SimpleStatement($"SELECT * FROM {table} LIMIT 10000")
                .SetPageSize(pageSize);

            var counter = 0;
            for (var i = 0; i < times; i++)
            {
                var rs = Session.Execute(statement);
                counter += rs.Count();
            }

            //Check that the sum of all rows in same thread is the same as total rows
            Assert.AreEqual(totalRowLength * times, counter);
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void Should_ReturnNextPage_When_SetPagingStateManually()
        {
            var pageSize = 10;
            var totalRowLength = 15;
            var table = CreateSimpleTableAndInsert(totalRowLength);
            var rs = Session.Execute(new SimpleStatement("SELECT * FROM " + table).SetAutoPage(false).SetPageSize(pageSize));
            Assert.NotNull(rs.PagingState);
            //It should have just the first page of rows
            Assert.AreEqual(pageSize, rs.InnerQueueCount);
            //Linq iteration should not make it to page
            Assert.AreEqual(pageSize, rs.Count());
            rs = Session.Execute(new SimpleStatement("SELECT * FROM " + table).SetAutoPage(false).SetPageSize(pageSize).SetPagingState(rs.PagingState));
            //It should only contain the following page rows
            Assert.AreEqual(totalRowLength - pageSize, rs.Count());
        }

        ////////////////////////////////////
        /// Test Helpers
        ////////////////////////////////////

        /// <summary>
        /// Creates a table and inserts a number of records synchronously.
        /// </summary>
        /// <returns>The name of the table</returns>
        private string CreateSimpleTableAndInsert(int rowsInTable, string tableName = null)
        {
            if (tableName == null)
            {
                tableName = TestUtils.GetUniqueTableName();
            }

            QueryTools.ExecuteSyncNonQuery(Session, $@"
                CREATE TABLE {tableName}(
                id uuid PRIMARY KEY,
                label text);");
            for (var i = 0; i < rowsInTable; i++)
            {
                Session.Execute(string.Format("INSERT INTO {2} (id, label) VALUES({0},'{1}')", Guid.NewGuid(), "LABEL" + i, tableName));
            }

            return tableName;
        }

        /// <summary>
        /// Creates a table with a composite index and inserts a number of records synchronously.
        /// </summary>
        /// <returns>The name of the table</returns>
        private Tuple<string, string> CreateTableWithCompositeIndexAndInsert(ISession session, int rowsInTable)
        {
            var tableName = TestUtils.GetUniqueTableName();
            var staticClusterKeyStr = "staticClusterKeyStr";
            QueryTools.ExecuteSyncNonQuery(session, $@"
                CREATE TABLE {tableName} (
                id text,
                label text,
                PRIMARY KEY (label, id));");
            for (var i = 0; i < rowsInTable; i++)
            {
                session.Execute(string.Format("INSERT INTO {2} (label, id) VALUES('{0}','{1}')", staticClusterKeyStr, Guid.NewGuid().ToString(), tableName));
            }
            var infoTuple = new Tuple<string, string>(tableName, staticClusterKeyStr);
            return infoTuple;
        }
    }
}
