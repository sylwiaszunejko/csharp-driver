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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Collections;
using Cassandra.Observers.Abstractions;
using Cassandra.Serialization;
using Cassandra.Tasks;

namespace Cassandra.Connections
{
    /// <summary>
    /// Represents a pool of connections to a host
    /// </summary>
    internal class HostConnectionPool : IHostConnectionPool
    {
        private static readonly Logger Logger = new Logger(typeof(HostConnectionPool));
        private Random rand = new Random();
        private const int ConnectionIndexOverflow = int.MaxValue - 1000000;
        private const long BetweenResizeDelay = 2000;

        /// <summary>
        /// Represents the possible states of the pool.
        /// Possible state transitions:
        ///  - From Init to Closing: The pool must be closed because the host is ignored or because the pool should
        ///    not attempt more reconnections (another pool is trying to reconnect to a UP host).
        ///  - From Init to ShuttingDown: The pool is being shutdown as a result of a client shutdown.
        ///  - From Closing to Init: The pool finished closing connections (is now ignored) and it resets to
        ///    initial state in case the host is marked as local/remote in the future.
        ///  - From Closing to ShuttingDown (rare): It was marked as ignored, now the client is being shutdown.
        ///  - From ShuttingDown to Shutdown: Finished shutting down, the pool should not be reused.
        /// </summary>
        private static class PoolState
        {
            /// <summary>
            /// Initial state: open / opening / ready to be opened
            /// </summary>
            public const int Init = 0;
            /// <summary>
            /// When the pool is being closed as part of a distance change
            /// </summary>
            public const int Closing = 1;
            /// <summary>
            /// When the pool is being shutdown for good
            /// </summary>
            public const int ShuttingDown = 2;
            /// <summary>
            /// When the pool has being shutdown
            /// </summary>
            public const int Shutdown = 3;
        }

        private readonly Configuration _config;
        private readonly ISerializerManager _serializerManager;
        private readonly IObserverFactory _observerFactory;
        private readonly CopyOnWriteShardedList<IConnection> _connections = new CopyOnWriteShardedList<IConnection>();
        private volatile int _connectionsPerShard;
        private volatile HostDistance _distance;
        private readonly HashedWheelTimer _timer;
        private readonly SemaphoreSlim _allConnectionClosedEventLock = new SemaphoreSlim(1, 1);
        private readonly Host _host;
        private volatile IReconnectionSchedule _reconnectionSchedule;
        private volatile int _expectedConnectionLength;
        private volatile int _maxInflightThresholdToConsiderResizing;
        private volatile int _maxConnectionLength;
        private volatile HashedWheelTimer.ITimeout _resizingEndTimeout;
        private volatile bool _canCreateForeground = true;
        private int _poolResizing;
        private int _state = PoolState.Init;
        private HashedWheelTimer.ITimeout _newConnectionTimeout;
        private TaskCompletionSource<IConnection> _connectionOpenTcs;
        private int _connectionIndex;
        private readonly int _maxRequestsPerConnection;
        private readonly PoolingOptions _poolingOptions;

        public event Action<Host, HostConnectionPool> AllConnectionClosed;

        /// <inheritdoc />
        public bool HasConnections => _connections.Count > 0;

        /// <inheritdoc />
        public int OpenConnections => _connections.Count;

        /// <inheritdoc />
        public int InFlight => _connections.Sum(c => c.InFlight);

        /// <summary>
        /// Determines whether the pool is not on the initial state.
        /// </summary>
        private bool IsClosing => Volatile.Read(ref _state) != PoolState.Init;

        /// <inheritdoc />
        public IConnection[] ConnectionsSnapshot => _connections.GetSnapshot().GetAllItems();

        private ShardingInfo shardingInfo { get; set; }

        private int lastAttemptedShard = 0;

        private TokenFactory _tokenFactory;

        public HostConnectionPool(Host host, Configuration config, ISerializerManager serializerManager, IObserverFactory observerFactory, TokenFactory tokenFactory)
        {
            _host = host;
            _host.Down += OnHostDown;
            _host.Up += OnHostUp;
            _host.DistanceChanged += OnDistanceChanged;
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _poolingOptions = config.GetOrCreatePoolingOptions(serializerManager.CurrentProtocolVersion);
            _maxRequestsPerConnection = _poolingOptions.GetMaxRequestsPerConnection();
            _serializerManager = serializerManager;
            _observerFactory = observerFactory;
            _timer = config.Timer;
            _reconnectionSchedule = config.Policies.ReconnectionPolicy.NewSchedule();
            _expectedConnectionLength = 1;
            _tokenFactory = tokenFactory;
        }

        /// <inheritdoc />
        public async Task<IConnection> BorrowConnectionAsync(RoutingKey routingKey = null, int shardID = -1)
        {
            var connections = await EnsureCreate().ConfigureAwait(false);
            if (connections.Length == 0)
            {
                throw new DriverInternalError("No connection could be borrowed");
            }

            return BorrowLeastBusyConnection(connections, routingKey, shardID);
        }

        /// <inheritdoc />
        public IConnection BorrowExistingConnection(RoutingKey routingKey, int shardID = -1)
        {
            var connections = GetExistingConnections();
            if (connections.Length == 0)
            {
                return null;
            }

            return BorrowLeastBusyConnection(connections, routingKey, shardID);
        }

        private IConnection BorrowLeastBusyConnection(ShardedList<IConnection> connections, RoutingKey routingKey = null, int shardID = -1)
        {
            if (shardingInfo != null)
            {
                if (routingKey != null)
                {
                    IToken token = _tokenFactory.Hash(routingKey.RawRoutingKey);
                    if (shardID == -1)
                    {
                        shardID = shardingInfo.ShardID(token);
                    }
                }
                else
                {
                    shardID = rand.Next(shardingInfo.ScyllaNrShards);
                }
            }

            IConnection c = null;
            if (shardID != -1)
            {
                var minInFlight = int.MaxValue;
                foreach (var connection in _connections.GetItemsForShard(shardID))
                {
                    int localInFlight = connection.InFlight;
                    if (localInFlight < minInFlight)
                    {
                        minInFlight = localInFlight;
                        c = connection;
                    }
                }
            }
            int inFlight;
            if (c != null)
            {
                // if we have a connection for the shard, use it if it is not too busy
                inFlight = c.InFlight;
                if (inFlight >= _maxRequestsPerConnection)
                {
                    c = HostConnectionPool.MinInFlight(connections, ref _connectionIndex, _maxRequestsPerConnection, out inFlight);
                }
            }
            else
            {
                c = HostConnectionPool.MinInFlight(connections, ref _connectionIndex, _maxRequestsPerConnection, out inFlight);
            }

            if (inFlight >= _maxRequestsPerConnection)
            {
                throw new BusyPoolException(_host.Address, _maxRequestsPerConnection, connections.Length);
            }

            ConsiderResizingPool(inFlight);
            return c;
        }

        private void CancelNewConnectionTimeout(HashedWheelTimer.ITimeout newTimeout = null)
        {
            var previousTimeout = Interlocked.Exchange(ref _newConnectionTimeout, newTimeout);
            if (previousTimeout != null)
            {
                // Clear previous reconnection attempt timeout
                previousTimeout.Cancel();
            }
            if (newTimeout != null && IsClosing)
            {
                // It could have been the case the it was set after it was set as closed.
                Interlocked.Exchange(ref _newConnectionTimeout, null);
            }
        }

        public void CheckHealth(IConnection c)
        {
            var timedOutOps = c.TimedOutOperations;
            if (timedOutOps < _config.SocketOptions.DefunctReadTimeoutThreshold)
            {
                return;
            }
            HostConnectionPool.Logger.Warning("Connection to {0} considered as unhealthy after {1} timed out operations",
                _host.Address, timedOutOps);
            Remove(c);
        }

        /// <inheritdoc />
        public void Remove(IConnection c)
        {
            if (c == null)
            {
                return;
            }

            if (c.IsDisposed)
            {
                OnConnectionClosing(c);
            }
            else
            {
                c.Dispose();
            }
        }

        public void ConsiderResizingPool(int inFlight)
        {
            if (inFlight < _maxInflightThresholdToConsiderResizing)
            {
                // The requests in-flight are normal
                return;
            }
            if (_expectedConnectionLength >= _maxConnectionLength)
            {
                // We can not add more connections
                return;
            }
            if (_connections.Count < _expectedConnectionLength)
            {
                // The pool is still trying to acquire the correct size
                return;
            }
            var canResize = Interlocked.Exchange(ref _poolResizing, 1) == 0;
            if (!canResize)
            {
                // There is already another thread resizing the pool
                return;
            }
            if (IsClosing)
            {
                return;
            }
            _expectedConnectionLength++;
            HostConnectionPool.Logger.Info("Increasing pool #{0} size to {1}, as in-flight requests are above threshold ({2})",
                GetHashCode(), _expectedConnectionLength, _maxInflightThresholdToConsiderResizing);
            StartCreatingConnection(null);
            _resizingEndTimeout = _timer.NewTimeout(_ => Interlocked.Exchange(ref _poolResizing, 0), null,
                HostConnectionPool.BetweenResizeDelay);
        }

        /// <summary>
        /// Releases the resources associated with the pool.
        /// </summary>
        public void Dispose()
        {
            var markShuttingDown =
                (Interlocked.CompareExchange(ref _state, PoolState.ShuttingDown, PoolState.Init) == PoolState.Init) ||
                (Interlocked.CompareExchange(ref _state, PoolState.ShuttingDown, PoolState.Closing) ==
                    PoolState.Closing);
            if (!markShuttingDown)
            {
                // The pool is already being shutdown, never mind
                return;
            }
            HostConnectionPool.Logger.Info("Disposing connection pool #{0} to {1}", GetHashCode(), _host.Address);
            var connections = _connections.ClearAndGet();
            foreach (var c in connections)
            {
                c.Dispose();
            }
            _host.Up -= OnHostUp;
            _host.Down -= OnHostDown;
            _host.DistanceChanged -= OnDistanceChanged;
            var t = _resizingEndTimeout;
            if (t != null)
            {
                t.Cancel();
            }
            CancelNewConnectionTimeout();
            Interlocked.Exchange(ref _state, PoolState.Shutdown);
        }

        public virtual async Task<IConnection> DoCreateAndOpen(bool isReconnection, int shardID = -1, int shardAwarePort = 0, int shardCount = 0)
        {
            IConnectionEndPoint endPoint;
            if (shardAwarePort != 0)
            {
                endPoint = await _config.EndPointResolver.GetConnectionShardAwareEndPointAsync(_host, isReconnection, shardAwarePort).ConfigureAwait(false);
            }
            else
            {
                endPoint = await _config.EndPointResolver.GetConnectionEndPointAsync(_host, isReconnection).ConfigureAwait(false);
            }
            var c = _config.ConnectionFactory.Create(_serializerManager.GetCurrentSerializer(), endPoint, _config, _observerFactory.CreateConnectionObserver(_host));
            c.Closing += OnConnectionClosing;
            if (_poolingOptions.GetHeartBeatInterval() > 0)
            {
                c.OnIdleRequestException += ex => OnIdleRequestException(c, ex);
            }
            try
            {
                if (shardID != -1)
                {
                    await c.Open(shardID, shardCount).ConfigureAwait(false);
                }
                else
                {
                    await c.Open().ConfigureAwait(false);
                }

            }
            catch
            {
                c.Dispose();
                throw;
            }
            return c;
        }

        public void OnHostRemoved()
        {
            var previousState = Interlocked.Exchange(ref _state, PoolState.ShuttingDown);
            if (previousState == PoolState.Shutdown)
            {
                // It was already shutdown
                Interlocked.Exchange(ref _state, PoolState.Shutdown);
                return;
            }
            HostConnectionPool.Logger.Info("Host decommissioned. Closing pool #{0} to {1}", GetHashCode(), _host.Address);

            DrainConnections(() => Interlocked.Exchange(ref _state, PoolState.Shutdown));

            CancelNewConnectionTimeout();
            var t = _resizingEndTimeout;
            if (t != null)
            {
                t.Cancel();
            }
        }

        /// <summary>
        /// Gets the connection with the minimum number of InFlight requests.
        /// Only checks for index + 1 and index, to avoid a loop of all connections.
        /// </summary>
        /// <param name="connections">A snapshot of the pool of connections</param>
        /// <param name="connectionIndex">Current round-robin index</param>
        /// <param name="inFlightThreshold">
        /// The max amount of in-flight requests that cause this method to continue
        /// iterating until finding the connection with min number of in-flight requests.
        /// </param>
        /// <param name="inFlight">
        /// Out parameter containing the amount of in-flight requests of the selected connection.
        /// </param>
        public static IConnection MinInFlight(ShardedList<IConnection> connections, ref int connectionIndex, int inFlightThreshold,
                                             out int inFlight)
        {
            if (connections.Length == 1)
            {
                inFlight = connections[0].InFlight;
                return connections[0];
            }
            //It is very likely that the amount of InFlight requests per connection is the same
            //Do round robin between connections, skipping connections that have more in flight requests
            var index = Interlocked.Increment(ref connectionIndex);
            if (index > HostConnectionPool.ConnectionIndexOverflow)
            {
                // Simplified overflow protection: once the threshold is reached, reset the shared reference
                // but still use the incremented value above threshold.
                // Multiple threads can reset it to 0 (in practice it would be very few), with the assumable side
                // effect of unbalancing the load between connections for a few moments.
                Interlocked.Exchange(ref connectionIndex, 0);
            }

            var c = connections[index % connections.Length];
            inFlight = 0;

            for (var i = 1; i < connections.Length; i++)
            {
                var nextConnection = connections[(index + i) % connections.Length];
                inFlight = c.InFlight;
                var nextInFlight = nextConnection.InFlight;

                if (inFlight > nextInFlight)
                {
                    c = nextConnection;
                    inFlight = nextInFlight;
                }

                if (inFlight < inFlightThreshold)
                {
                    // We should avoid traversing all the connections
                    // We have a connection with a decent amount of in-flight requests
                    break;
                }
            }

            return c;
        }

        internal void OnConnectionClosing(IConnection c = null)
        {
            int currentLength;
            if (c != null)
            {
                var removalInfo = _connections.RemoveAndCount(c);
                currentLength = removalInfo.Item2;
                var removed = removalInfo.Item1;
                if (!removed)
                {
                    // It has been already removed (via event or direct call)
                    // When it was removed, all the following checks have been made
                    // No point in doing them again
                    return;
                }
                c.Dispose();
                HostConnectionPool.Logger.Info("Pool #{0} for host {1} removed a connection, new length: {2}",
                    GetHashCode(), _host.Address, currentLength);
            }
            else
            {
                currentLength = _connections.Count;
            }
            if (IsClosing || currentLength >= _expectedConnectionLength)
            {
                // No need to reconnect
                return;
            }
            // We are using an IO thread
            Task.Run(async () =>
            {
                // Use a lock for avoiding concurrent calls to SetNewConnectionTimeout()
                await _allConnectionClosedEventLock.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (currentLength == 0)
                    {
                        // All connections have been closed
                        // If the node is UP, we should stop attempting to reconnect
                        if (_host.IsUp && AllConnectionClosed != null)
                        {
                            // Raise the event and wait for a caller to decide
                            AllConnectionClosed(_host, this);
                            return;
                        }
                    }
                    SetNewConnectionTimeout(_reconnectionSchedule);
                }
                finally
                {
                    _allConnectionClosedEventLock.Release();
                }
            }).Forget();
        }

        private void OnDistanceChanged(HostDistance previousDistance, HostDistance distance)
        {
            SetDistance(distance);
            if (previousDistance == HostDistance.Ignored)
            {
                _canCreateForeground = true;
                // Start immediate reconnection
                ScheduleReconnection(true);
                return;
            }
            if (distance != HostDistance.Ignored)
            {
                return;
            }
            // Host is now ignored
            var isClosing = Interlocked.CompareExchange(ref _state, PoolState.Closing, PoolState.Init) ==
                PoolState.Init;
            if (!isClosing)
            {
                // Is already shutting down or shutdown, don't mind
                return;
            }
            HostConnectionPool.Logger.Info("Host ignored. Closing pool #{0} to {1}", GetHashCode(), _host.Address);
            DrainConnections(() =>
            {
                // After draining, set the pool back to init state
                Interlocked.CompareExchange(ref _state, PoolState.Init, PoolState.Closing);
            });
            CancelNewConnectionTimeout();
        }

        /// <summary>
        /// Removes the connections from the pool and defers the closing of the connections until twice the
        /// readTimeout. The connection might be already selected and sending requests.
        /// </summary>
        private void DrainConnections(Action afterDrainHandler)
        {
            var connections = _connections.ClearAndGet();
            if (connections.Length == 0)
            {
                HostConnectionPool.Logger.Info("Pool #{0} to {1} had no connections", GetHashCode(), _host.Address);
                return;
            }
            // The request handler might execute up to 2 queries with a single connection:
            // Changing the keyspace + the actual query
            var delay = _config.SocketOptions.ReadTimeoutMillis * 2;
            // Use a sane maximum of 5 mins
            const int maxDelay = 5 * 60 * 1000;
            if (delay <= 0 || delay > maxDelay)
            {
                delay = maxDelay;
            }
            DrainConnectionsTimer(connections, afterDrainHandler, delay / 1000);
        }

        private void DrainConnectionsTimer(ShardedList<IConnection> connections, Action afterDrainHandler, int steps)
        {
            _timer.NewTimeout(_ =>
            {
                Task.Run(() =>
                {
                    var drained = !connections.Any(c => c.HasPendingOperations);
                    if (!drained && --steps >= 0)
                    {
                        HostConnectionPool.Logger.Info("Pool #{0} to {1} can not be closed yet",
                            GetHashCode(), _host.Address);
                        DrainConnectionsTimer(connections, afterDrainHandler, steps);
                        return;
                    }
                    HostConnectionPool.Logger.Info("Pool #{0} to {1} closing {2} connections to after {3} draining",
                        GetHashCode(), _host.Address, connections.Length, drained ? "successful" : "unsuccessful");
                    foreach (var c in connections)
                    {
                        c.Dispose();
                    }
                    afterDrainHandler?.Invoke();
                });
            }, null, 1000);
        }

        public void OnHostUp(Host h)
        {
            // It can be awaited upon pool creation
            _canCreateForeground = true;
            if (_connections.Count > 0)
            {
                // This was the pool that was reconnecting, the pool is already getting the appropriate size
                return;
            }
            HostConnectionPool.Logger.Info("Pool #{0} for host {1} attempting to reconnect as host is UP", GetHashCode(), _host.Address);
            // Schedule an immediate reconnection
            ScheduleReconnection(true);
        }

        private void OnHostDown(Host h)
        {
            // Cancel the outstanding timeout (if any)
            // If the timeout already elapsed, a connection could be been created anyway
            CancelNewConnectionTimeout();
        }

        /// <summary>
        /// Handler that gets invoked when if there is a socket exception when making a heartbeat/idle request
        /// </summary>
        private void OnIdleRequestException(IConnection c, Exception ex)
        {
            if (c.IsDisposed)
            {
                HostConnectionPool.Logger.Info("Idle timeout exception, connection to {0} is disposed. Exception: {1}",
                    _host.Address, ex);
            }
            else
            {
                HostConnectionPool.Logger.Warning("Connection to {0} considered as unhealthy after idle timeout exception: {1}",
                    _host.Address, ex);
                c.Close();
            }
        }

        /// <inheritdoc />
        public void ScheduleReconnection(bool immediate = false)
        {
            var schedule = _config.Policies.ReconnectionPolicy.NewSchedule();
            _reconnectionSchedule = schedule;
            SetNewConnectionTimeout(immediate ? null : schedule);
        }

        private void SetNewConnectionTimeout(IReconnectionSchedule schedule)
        {
            if (schedule != null && _reconnectionSchedule != schedule)
            {
                // There's another reconnection schedule, leave it
                return;
            }
            HashedWheelTimer.ITimeout timeout = null;
            if (schedule != null)
            {
                // Schedule the creation
                var delay = schedule.NextDelayMs();
                HostConnectionPool.Logger.Info("Scheduling reconnection from #{0} to {1} in {2}ms", GetHashCode(), _host.Address, delay);
                timeout = _timer.NewTimeout(_ => Task.Run(() => StartCreatingConnection(schedule)), null, delay);
            }
            CancelNewConnectionTimeout(timeout);
            if (schedule == null)
            {
                // Start creating immediately after de-scheduling the timer
                HostConnectionPool.Logger.Info("Starting reconnection from pool #{0} to {1}", GetHashCode(), _host.Address);
                StartCreatingConnection(null);
            }
        }

        /// <summary>
        /// Asynchronously starts to create a new connection (if its not already being created).
        /// A <c>null</c> schedule signals that the pool is not reconnecting but growing to the expected size.
        /// </summary>
        /// <param name="schedule"></param>
        private void StartCreatingConnection(IReconnectionSchedule schedule)
        {
            var count = _connections.Count;
            if (count >= _expectedConnectionLength)
            {
                return;
            }
            if (schedule != null && schedule != _reconnectionSchedule)
            {
                // There's another reconnection schedule, leave it
                return;
            }

            CreateOrScheduleReconnectAsync(schedule).Forget();
        }

        private async Task CreateOrScheduleReconnectAsync(IReconnectionSchedule schedule)
        {
            if (IsClosing)
            {
                return;
            }

            try
            {
                var t = await CreateOpenConnection(false, schedule != null).ConfigureAwait(false);
                UpdateShardingInfo(t);
                StartCreatingConnection(null);
                _host.BringUpIfDown();
            }
            catch (Exception)
            {
                // The connection could not be opened
                if (IsClosing)
                {
                    // don't mind, the pool is not supposed to be open
                    return;
                }

                if (schedule == null)
                {
                    // As it failed, we need a new schedule for the following attempts
                    schedule = _config.Policies.ReconnectionPolicy.NewSchedule();
                    _reconnectionSchedule = schedule;
                }

                if (schedule != _reconnectionSchedule)
                {
                    // There's another reconnection schedule, leave it
                    return;
                }

                OnConnectionClosing();
            }
        }

        /// <summary>
        /// Opens one connection.
        /// If a connection is being opened it yields the same task, preventing creation in parallel.
        /// </summary>
        /// <param name="satisfyWithAnOpenConnection">
        /// Determines whether the Task should be marked as completed when there is a connection already opened.
        /// </param>
        /// <param name="isReconnection">Determines whether this is a reconnection</param>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException" />
        private async Task<IConnection> CreateOpenConnection(bool satisfyWithAnOpenConnection, bool isReconnection)
        {
            var concurrentOpenTcs = Volatile.Read(ref _connectionOpenTcs);
            // Try to exit early (cheap) as there could be another thread creating / finishing creating
            if (concurrentOpenTcs != null)
            {
                // There is another thread opening a new connection
                return await concurrentOpenTcs.Task.ConfigureAwait(false);
            }
            var tcs = new TaskCompletionSource<IConnection>();
            // Try to set the creation task source
            concurrentOpenTcs = Interlocked.CompareExchange(ref _connectionOpenTcs, tcs, null);
            if (concurrentOpenTcs != null)
            {
                // There is another thread opening a new connection
                return await concurrentOpenTcs.Task.ConfigureAwait(false);
            }

            if (IsClosing)
            {
                return await FinishOpen(tcs, false, HostConnectionPool.GetNotConnectedException()).ConfigureAwait(false);
            }

            // Before creating, make sure that its still needed
            // This method is the only one that adds new connections
            // But we don't control the removal, use snapshot
            var connectionsSnapshot = _connections.GetSnapshot();
            if (connectionsSnapshot.Length >= _expectedConnectionLength)
            {
                if (connectionsSnapshot.Length == 0)
                {
                    // Avoid race condition while removing
                    return await FinishOpen(tcs, false, HostConnectionPool.GetNotConnectedException()).ConfigureAwait(false);
                }
                return await FinishOpen(tcs, true, null, connectionsSnapshot[0]).ConfigureAwait(false);
            }

            if (satisfyWithAnOpenConnection && !_canCreateForeground)
            {
                // We only care about a single connection, if its already there, yield it
                connectionsSnapshot = _connections.GetSnapshot();
                if (connectionsSnapshot.Length == 0)
                {
                    // When creating in foreground, it failed
                    return await FinishOpen(tcs, false, HostConnectionPool.GetNotConnectedException()).ConfigureAwait(false);
                }
                return await FinishOpen(tcs, false, null, connectionsSnapshot[0]).ConfigureAwait(false);
            }

            HostConnectionPool.Logger.Info("Creating a new connection to {0}", _host.Address);
            IConnection c;
            try
            {
                // Find out to which shard should we connect to
                var shardID = -1;
                var shardAwarePort = 0;
                var shardCount = 0;
                if (shardingInfo != null)
                {
                    shardAwarePort = _config.ProtocolOptions.SslOptions != null ? shardingInfo.ScyllaShardAwarePortSSL : shardingInfo.ScyllaShardAwarePort;
                    shardCount = shardingInfo.ScyllaNrShards;
                    // Find the shard without a connection
                    // It's important to start counting from 1 here because we want
                    // to consider the next shard after the previously attempted one
                    for (var i = 1; i <= shardCount; i++)
                    {
                        var _shardID = (lastAttemptedShard + i) % shardCount;
                        if (_connections.Length > _shardID && _connections.GetItemsForShard(_shardID).Length < _connectionsPerShard)
                        {
                            lastAttemptedShard = _shardID;
                            shardID = _shardID;
                            break;
                        }
                    }
                }
                c = await DoCreateAndOpen(isReconnection, shardID, shardAwarePort, shardCount).ConfigureAwait(false);
                if (c != null && c.ShardID != -1)
                {
                    lastAttemptedShard = c.ShardID;
                }
            }
            catch (Exception ex)
            {
                HostConnectionPool.Logger.Info("Connection to {0} could not be created: {1}", _host.Address, ex);
                return await FinishOpen(tcs, true, ex).ConfigureAwait(false);
            }

            if (IsClosing)
            {
                HostConnectionPool.Logger.Info("Connection to {0} opened successfully but pool #{1} was being closed",
                    _host.Address, GetHashCode());
                c.Dispose();
                return await FinishOpen(tcs, false, HostConnectionPool.GetNotConnectedException()).ConfigureAwait(false);
            }

            var newLength = _connections.AddNew(c);
            HostConnectionPool.Logger.Info("Connection to {0} opened successfully, pool #{1} length: {2}",
                _host.Address, GetHashCode(), newLength);

            if (IsClosing)
            {
                // We haven't use a CAS operation, so it's possible that the pool is being closed while adding a new
                // connection, we should remove it.
                HostConnectionPool.Logger.Info("Connection to {0} opened successfully and added to the pool #{1} but the pool was being closed",
                    _host.Address, GetHashCode());
                _connections.Remove(c);
                c.Dispose();
                return await FinishOpen(tcs, false, HostConnectionPool.GetNotConnectedException()).ConfigureAwait(false);
            }

            if (c.IsDisposed)
            {
                // We haven't use a CAS operation, so it's possible that the connection is being disposed while adding it
                HostConnectionPool.Logger.Info("Connection to {0} opened successfully and added to the pool #{1} but it got closed",
                    _host.Address, GetHashCode());
                _connections.Remove(c);
                c.Dispose();
                return await FinishOpen(tcs, true, HostConnectionPool.GetNotConnectedException()).ConfigureAwait(false);
            }

            return await FinishOpen(tcs, true, null, c).ConfigureAwait(false);
        }

        private Task<IConnection> FinishOpen(
            TaskCompletionSource<IConnection> tcs,
            bool preventForeground,
            Exception ex,
            IConnection c = null)
        {
            // Instruction ordering: canCreateForeground flag must be set before resetting of the tcs
            if (preventForeground)
            {
                _canCreateForeground = false;
            }
            Interlocked.Exchange(ref _connectionOpenTcs, null);
            tcs.TrySet(ex, c);
            return tcs.Task;
        }

        private static SocketException GetNotConnectedException()
        {
            return new SocketException((int)SocketError.NotConnected);
        }

        /// <summary>
        /// Ensures that the pool has at least contains 1 connection to the host.
        /// </summary>
        /// <returns>An Array of connections with 1 or more elements or throws an exception.</returns>
        /// <exception cref="SocketException" />
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException" />
        public async Task<ShardedList<IConnection>> EnsureCreate()
        {
            var connections = GetExistingConnections();
            if (connections.Length > 0)
            {
                // Use snapshot to return as early as possible
                return connections;
            }

            if (!_canCreateForeground)
            {
                // Take a new snapshot
                connections = _connections.GetSnapshot();
                if (connections.Length > 0)
                {
                    return connections;
                }
                // It's not considered as connected
                throw HostConnectionPool.GetNotConnectedException();
            }
            IConnection c;
            try
            {
                // It should only await for the creation of the connection in few selected occasions:
                // It's the first time accessing or it has been recently set as UP
                // CreateOpenConnection() supports concurrent calls
                c = await CreateOpenConnection(true, false).ConfigureAwait(false);
                UpdateShardingInfo(c);
            }
            catch (Exception)
            {
                OnConnectionClosing();
                throw;
            }
            StartCreatingConnection(null);
            return new ShardedList<IConnection>(new[] { c });
        }

        /// <summary>
        /// Gets existing connections snapshot.
        /// If it's empty then it validates whether the pool is shutting down or the is down (in which case an exception is thrown).
        /// </summary>
        /// <exception cref="SocketException">Not connected.</exception>
        private ShardedList<IConnection> GetExistingConnections()
        {
            var connections = _connections.GetSnapshot();
            if (connections.Count > 0)
            {
                return connections;
            }

            if (IsClosing || !_host.IsUp)
            {
                // Should have not been considered as UP
                throw HostConnectionPool.GetNotConnectedException();
            }

            return connections;
        }

        public void SetDistance(HostDistance distance)
        {
            _distance = distance;
            _expectedConnectionLength = _poolingOptions.GetCoreConnectionsPerHost(distance);
            _maxInflightThresholdToConsiderResizing = _poolingOptions.GetMaxSimultaneousRequestsPerConnectionTreshold(distance);
            _maxConnectionLength = _poolingOptions.GetMaxConnectionPerHost(distance);
        }

        /// <inheritdoc />
        public void MarkAsDownAndScheduleReconnection()
        {
            // By setting the host as down, all pools should cancel any outstanding reconnection attempt
            if (_host.SetDown())
            {
                // Only attempt reconnection with 1 connection pool
                ScheduleReconnection();
            }
        }

        /// <inheritdoc />
        public Task<IConnection> GetConnectionFromHostAsync(
            IDictionary<IPEndPoint, Exception> triedHosts, Func<string> getKeyspaceFunc, RoutingKey routingKey, int shardID = -1)
        {
            return GetConnectionFromHostAsync(triedHosts, getKeyspaceFunc, true, routingKey, shardID);
        }

        /// <inheritdoc />
        public Task<IConnection> GetExistingConnectionFromHostAsync(
            IDictionary<IPEndPoint, Exception> triedHosts, Func<string> getKeyspaceFunc, RoutingKey routingKey, int shardID = -1)
        {
            return GetConnectionFromHostAsync(triedHosts, getKeyspaceFunc, false, routingKey, shardID);
        }

        private async Task<IConnection> GetConnectionFromHostAsync(
            IDictionary<IPEndPoint, Exception> triedHosts, Func<string> getKeyspaceFunc, bool createIfNeeded, RoutingKey routingKey, int shardID = -1)
        {
            IConnection c = null;
            try
            {
                if (createIfNeeded)
                {
                    c = await BorrowConnectionAsync(routingKey, shardID).ConfigureAwait(false);
                }
                else
                {
                    c = BorrowExistingConnection(routingKey, shardID);
                }
            }
            catch (UnsupportedProtocolVersionException ex)
            {
                // The version of the protocol is not supported on this host
                // Most likely, we are using a higher protocol version than the host supports
                HostConnectionPool.Logger.Error("Host {0} does not support protocol version {1}. You should use a fixed protocol " +
                             "version during rolling upgrades of the cluster. Setting the host as DOWN to " +
                             "avoid hitting this node as part of the query plan for a while", _host.Address, ex.ProtocolVersion);
                triedHosts[_host.Address] = ex;
                MarkAsDownAndScheduleReconnection();
            }
            catch (BusyPoolException ex)
            {
                HostConnectionPool.Logger.Warning(
                    "All connections to host {0} are busy ({1} requests are in-flight on {2} connection(s))," +
                    " consider lowering the pressure or make more nodes available to the client", _host.Address,
                    ex.MaxRequestsPerConnection, ex.ConnectionLength);
                triedHosts[_host.Address] = ex;
            }
            catch (Exception ex)
            {
                // Probably a SocketException/AuthenticationException, move along
                if (!_canCreateForeground && createIfNeeded)
                {
                    HostConnectionPool.Logger.Verbose("Could not open a connection to {0} " +
                                                      "because the pool is not allowing creation of connections in the foreground. " +
                                                      "Exception: {1}", _host.Address, ex.ToString());
                }
                else
                {
                    HostConnectionPool.Logger.Warning("Exception while trying borrow a connection from a pool: {0}", ex.ToString());
                }
                triedHosts[_host.Address] = ex;
            }

            if (c == null)
            {
                return null;
            }

            try
            {
                await c.SetKeyspace(getKeyspaceFunc()).ConfigureAwait(false);
            }
            catch (SocketException)
            {
                Remove(c);
                throw;
            }

            return c;
        }

        private void UpdateShardingInfo(IConnection c)
        {
            if (!_poolingOptions.GetDisableShardAwareness() && shardingInfo == null && c.ShardingInfo() != null)
            {
                shardingInfo = c.ShardingInfo();
                var coreSize = _poolingOptions.GetCoreConnectionsPerHost(_distance);
                var shardsCount = shardingInfo == null ? 1 : shardingInfo.ScyllaNrShards;

                _connectionsPerShard = coreSize / shardsCount + (coreSize % shardsCount > 0 ? 1 : 0);
                _expectedConnectionLength = shardsCount * _connectionsPerShard;
            }
        }

        /// <summary>
        /// Creates the required connections to the hosts and awaits for all connections to be open.
        /// The task is completed when at least 1 of the connections is opened successfully.
        /// Until the task is completed, no other thread is expected to be using this instance.
        /// </summary>
        public async Task Warmup()
        {
            try
            {
                var c = await CreateOpenConnection(false, false).ConfigureAwait(false);
                UpdateShardingInfo(c);
            }
            catch
            {
                OnConnectionClosing();
                throw;
            }

            var length = _expectedConnectionLength;

            for (var i = 1; i < length; i++)
            {
                try
                {
                    await CreateOpenConnection(false, false).ConfigureAwait(false);
                }
                catch
                {
                    break;
                }
            }
        }
    }
}
