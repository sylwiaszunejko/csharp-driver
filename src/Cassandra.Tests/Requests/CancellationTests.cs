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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Connections;
using Cassandra.Observers.Abstractions;
using Cassandra.Observers.Null;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tests.Connections.TestHelpers;

using Moq;

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.Tests.Requests
{
    [TestFixture]
    public class CancellationTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        [Test]
        public async Task RequestHandler_Should_Send_Nothing_When_The_Token_Is_Already_Cancelled()
        {
            using var setup = BuildRequestHandler();
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var task = setup.RequestHandler.SendAsync(cts.Token);

            var ex = await Assert.ThrowsAsync<TaskCanceledException>(async () => await task.WaitAsync(Timeout).ConfigureAwait(false));
            Assert.AreEqual(cts.Token, ex.CancellationToken);
            Assert.IsTrue(task.IsCanceled);
            await Task.Delay(100).ConfigureAwait(false);
            Assert.AreEqual(0, setup.Operations.Count);
        }

        [Test]
        public async Task RequestHandler_Should_Complete_As_Cancelled_And_Stop_Speculative_Executions_When_Cancelled_After_Sending()
        {
            // The node never responds, and a speculative execution would start 100 ms after the first one
            using var setup = BuildRequestHandler(speculativeDelayMillis: 100);
            using var cts = new CancellationTokenSource();

            var task = setup.RequestHandler.SendAsync(cts.Token);
            var operation = await setup.FirstOperation.Task.WaitAsync(Timeout).ConfigureAwait(false);
            cts.Cancel();

            var ex = await Assert.ThrowsAsync<TaskCanceledException>(async () => await task.WaitAsync(Timeout).ConfigureAwait(false));
            Assert.AreEqual(cts.Token, ex.CancellationToken);
            Assert.IsTrue(task.IsCanceled);

            // The request, still in the write queue, is cancelled, and no other execution is started
            Assert.IsFalse(operation.CanBeWritten());
            await Task.Delay(300).ConfigureAwait(false);
            Assert.AreEqual(1, setup.Operations.Count);
        }

        [Test]
        public async Task RequestHandler_Should_Leave_A_Written_Request_Running_When_Cancelled()
        {
            using var setup = BuildRequestHandler();
            using var cts = new CancellationTokenSource();

            var task = setup.RequestHandler.SendAsync(cts.Token);
            var operation = await setup.FirstOperation.Task.WaitAsync(Timeout).ConfigureAwait(false);
            operation.WriteFrame(1, new MemoryStream(), setup.Serializer, false, 0);
            cts.Cancel();

            await Assert.ThrowsAsync<TaskCanceledException>(async () => await task.WaitAsync(Timeout).ConfigureAwait(false));

            // Not cancelled, so that its read timeout still runs; its response is ignored
            Assert.IsTrue(operation.CanBeWritten());
            await setup.Respond().ConfigureAwait(false);
            Assert.IsTrue(task.IsCanceled);
        }

        [Test]
        public async Task RequestHandler_Should_Not_Cancel_A_Keyspace_Switch_When_Cancelled()
        {
            // Cancelling a keyspace switch closes its connection, which other requests share
            using var setup = BuildRequestHandler(statement: new SimpleStatement("USE ks1"));
            using var cts = new CancellationTokenSource();

            var task = setup.RequestHandler.SendAsync(cts.Token);
            var operation = await setup.FirstOperation.Task.WaitAsync(Timeout).ConfigureAwait(false);
            cts.Cancel();

            await Assert.ThrowsAsync<TaskCanceledException>(async () => await task.WaitAsync(Timeout).ConfigureAwait(false));
            Assert.IsTrue(operation.CanBeWritten());
        }

        [Test]
        public async Task RequestHandler_Should_Complete_As_Cancelled_When_Cancelled_While_Waiting_For_Schema_Agreement()
        {
            using var setup = BuildRequestHandler();
            using var cts = new CancellationTokenSource();
            var schemaAgreement = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var task = setup.RequestHandler.SendAsync(cts.Token);
            await setup.FirstOperation.Task.WaitAsync(Timeout).ConfigureAwait(false);
            // A schema change response completes the handler, then waits for schema agreement before the result
            Assert.IsTrue(await setup.RequestHandler.SetCompletedAsync(new RowSet(), () => schemaAgreement.Task).ConfigureAwait(false));
            cts.Cancel();

            var ex = await Assert.ThrowsAsync<TaskCanceledException>(async () => await task.WaitAsync(Timeout).ConfigureAwait(false));
            Assert.AreEqual(cts.Token, ex.CancellationToken);
            schemaAgreement.SetResult();
        }

        [Test]
        public async Task ResultHandler_Should_Complete_As_Cancelled_When_The_Request_Observer_Throws()
        {
            var observer = new Mock<IRequestObserver>();
            observer
                .Setup(o => o.OnRequestFailureAsync(It.IsAny<Exception>(), It.IsAny<SessionRequestInfo>()))
                .ThrowsAsync(new InvalidOperationException("Observer failure"));
            var resultHandler = new TcsMetricsRequestResultHandler(observer.Object);
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => resultHandler.TrySetCanceledAsync(cts.Token, new SessionRequestInfo(Mock.Of<IStatement>(), null)));

            Assert.IsTrue(resultHandler.Task.IsCanceled);
        }

        [Test]
        public async Task ResultHandler_Should_Fault_With_An_OperationCanceledException_That_Is_Not_The_Callers()
        {
            // An OperationCanceledException thrown inside the driver, for instance by a policy, is an error
            var resultHandler = new TcsMetricsRequestResultHandler(Mock.Of<IRequestObserver>());
            var error = new OperationCanceledException("Policy failure");

            await resultHandler.TrySetExceptionAsync(error, new SessionRequestInfo(Mock.Of<IStatement>(), null)).ConfigureAwait(false);

            Assert.IsTrue(resultHandler.Task.IsFaulted);
            Assert.AreSame(error, resultHandler.Task.Exception.InnerException);
        }

        [Test]
        public async Task RequestHandler_Should_Return_The_Result_When_The_Token_Is_Not_Cancelled()
        {
            using var setup = BuildRequestHandler();
            using var cts = new CancellationTokenSource();

            var task = setup.RequestHandler.SendAsync(cts.Token);
            await setup.FirstOperation.Task.WaitAsync(Timeout).ConfigureAwait(false);
            await setup.Respond().ConfigureAwait(false);

            Assert.IsNotNull(await task.WaitAsync(Timeout).ConfigureAwait(false));

            // Cancelling after the request completed has no effect
            cts.Cancel();
            Assert.IsTrue(task.IsCompletedSuccessfully);
        }

        [Test]
        public async Task ExecuteAsync_Should_Not_Create_A_Request_Handler_When_The_Token_Is_Already_Cancelled()
        {
            var requestHandlerFactory = new Mock<IRequestHandlerFactory>(MockBehavior.Strict);
            var session = BuildSession(requestHandlerFactory.Object, Mock.Of<IInternalCluster>());
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var task = session.ExecuteAsync(new SimpleStatement("SELECT * FROM tbl"), cts.Token);

            Assert.IsTrue(task.IsCanceled);
            var ex = await Assert.ThrowsAsync<TaskCanceledException>(async () => await task.ConfigureAwait(false));
            Assert.AreEqual(cts.Token, ex.CancellationToken);
        }

        [Test]
        public void ExecuteAsync_Should_Throw_For_An_Unknown_Execution_Profile_Even_When_The_Token_Is_Cancelled()
        {
            var session = BuildSession(Mock.Of<IRequestHandlerFactory>(), Mock.Of<IInternalCluster>());
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            Assert.Throws<ArgumentException>(
                () => session.ExecuteAsync(new SimpleStatement("SELECT * FROM tbl"), "unknown", cts.Token));
        }

        [Test]
        public void PrepareAsync_Should_Not_Prepare_When_The_Token_Is_Already_Cancelled()
        {
            var cluster = new Mock<IInternalCluster>();
            var session = BuildSession(Mock.Of<IRequestHandlerFactory>(), cluster.Object);
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var task = session.PrepareAsync("SELECT * FROM tbl", cts.Token);

            Assert.IsTrue(task.IsCanceled);
            cluster.Verify(
                c => c.Prepare(It.IsAny<IInternalSession>(), It.IsAny<ISerializerManager>(), It.IsAny<InternalPrepareRequest>()),
                Times.Never);
        }

        [Test]
        public async Task PrepareAsync_Should_Stop_Waiting_Without_Stopping_The_Shared_Preparation_When_Cancelled()
        {
            var preparation = new TaskCompletionSource<PreparedStatement>(TaskCreationOptions.RunContinuationsAsynchronously);
            var cluster = new Mock<IInternalCluster>();
            cluster
                .Setup(c => c.Prepare(It.IsAny<IInternalSession>(), It.IsAny<ISerializerManager>(), It.IsAny<InternalPrepareRequest>()))
                .Returns(preparation.Task);
            var session = BuildSession(Mock.Of<IRequestHandlerFactory>(), cluster.Object);
            using var cts = new CancellationTokenSource();

            var task = session.PrepareAsync("SELECT * FROM tbl", cts.Token);
            cts.Cancel();

            var ex = await Assert.ThrowsAsync<TaskCanceledException>(async () => await task.WaitAsync(Timeout).ConfigureAwait(false));
            Assert.AreEqual(cts.Token, ex.CancellationToken);
            Assert.IsFalse(preparation.Task.IsCompleted);

            // Other callers sharing the preparation still get its result
            var prepared = new PreparedStatement();
            preparation.SetResult(prepared);
            Assert.AreSame(prepared, await session.PrepareAsync("SELECT * FROM tbl").ConfigureAwait(false));
        }

        private static Session BuildSession(IRequestHandlerFactory requestHandlerFactory, IInternalCluster cluster)
        {
            var config = new TestConfigurationBuilder
            {
                RequestHandlerFactory = requestHandlerFactory,
                HostConnectionPoolFactory = Mock.Of<IHostConnectionPoolFactory>()
            }.Build();
            Mock.Get(cluster).Setup(c => c.Configuration).Returns(config);
            return new Session(cluster, config, null, SerializerManager.Default, null);
        }

        private static RequestHandlerSetup BuildRequestHandler(long speculativeDelayMillis = 0, IStatement statement = null)
        {
            var connection = Mock.Of<IConnection>();
            var setup = new RequestHandlerSetup();
            var speculativeExecutionPolicy = speculativeDelayMillis > 0
                ? (ISpeculativeExecutionPolicy)new ConstantSpeculativeExecutionPolicy(speculativeDelayMillis, 2)
                : NoSpeculativeExecutionPolicy.Instance;
            var config = new TestConfigurationBuilder
            {
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(() => connection),
                Policies = new Cassandra.Policies(
                    new RoundRobinPolicy(),
                    new ConstantReconnectionPolicy(100),
                    new DefaultRetryPolicy(),
                    speculativeExecutionPolicy,
                    new AtomicMonotonicTimestampGenerator(),
                    null)
            }.Build();
            var initializer = Mock.Of<IInitializer>();
            Mock.Get(initializer).Setup(i => i.ContactPoints).Returns(new List<IPEndPoint>
            {
                new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042),
                new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042)
            });
            Mock.Get(initializer).Setup(i => i.GetConfiguration()).Returns(config);
            var cluster = Cluster.BuildFrom(initializer, new List<string>());
            setup.Cluster = cluster;
            cluster.Connect();
            var session = new Session(cluster, config, null, SerializerManager.Default, null);
            setup.Session = session;

            // The node only responds when the test calls Respond
            Mock.Get(connection)
                .Setup(c => c.SendWithKeyspace(
                    It.IsAny<IRequest>(),
                    It.IsAny<string>(),
                    It.IsAny<Func<IRequestError, Response, Task>>(),
                    It.IsAny<int>(),
                    It.IsAny<bool>()))
                .Returns<IRequest, string, Func<IRequestError, Response, Task>, int, bool>((request, _, callback, timeout, __) =>
                {
                    var operation = new OperationState(
                        (error, response) => callback(error, response), request, timeout, NullOperationObserver.Instance);
                    setup.Operations.Enqueue(operation);
                    setup.Callbacks.Enqueue(callback);
                    setup.FirstOperation.TrySetResult(operation);
                    return Task.FromResult(operation);
                });
            Mock.Get(connection)
                .SetupGet(c => c.EndPoint)
                .Returns(new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042), config.ServerNameResolver, null));

            statement ??= new SimpleStatement("SELECT * FROM tbl").SetIdempotence(true);
            var trackingInfoAndObserver = RequestHandler.CreateRequestObserver(session, statement).GetAwaiter().GetResult();
            setup.RequestHandler = new RequestHandler(
                session,
                setup.Serializer,
                trackingInfoAndObserver.Item1,
                config.DefaultRequestOptions,
                trackingInfoAndObserver.Item2);
            return setup;
        }

        private sealed class RequestHandlerSetup : IDisposable
        {
            public IRequestHandler RequestHandler { get; set; }

            public ISerializer Serializer { get; } = new SerializerManager(ProtocolVersion.V4).GetCurrentSerializer();

            public Cluster Cluster { get; set; }

            public Session Session { get; set; }

            public ConcurrentQueue<OperationState> Operations { get; } = new ConcurrentQueue<OperationState>();

            public ConcurrentQueue<Func<IRequestError, Response, Task>> Callbacks { get; } =
                new ConcurrentQueue<Func<IRequestError, Response, Task>>();

            public TaskCompletionSource<OperationState> FirstOperation { get; } =
                new TaskCompletionSource<OperationState>(TaskCreationOptions.RunContinuationsAsynchronously);

            public Task Respond()
            {
                Callbacks.TryPeek(out var callback);
                return callback(null, new ProxyResultResponse(ResultResponse.ResultResponseKind.Void));
            }

            public void Dispose()
            {
                Session?.Dispose();
                Cluster?.Dispose();
            }
        }
    }
}
