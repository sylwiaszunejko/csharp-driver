﻿//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Observers.Abstractions;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    internal class ReprepareHandler : IReprepareHandler
    {
        /// <inheritdoc />
        public async Task ReprepareOnAllNodesWithExistingConnections(
            IInternalSession session, InternalPrepareRequest request, PrepareResult prepareResult, IRequestObserver observer, SessionRequestInfo sessionRequestInfo)
        {
            var pools = session.GetPools();
            var hosts = session.InternalCluster.AllHosts();
            var poolsByHosts = pools.Join(
                hosts, po => po.Key,
                h => h.Address,
                (pair, host) => new { host, pair.Value }).ToDictionary(k => k.host, k => k.Value);

            if (poolsByHosts.Count == 0)
            {
                PrepareHandler.Logger.Warning("Could not prepare query on all hosts because there are no connection pools.");
                return;
            }

            using (var semaphore = new SemaphoreSlim(64, 64))
            {
                var tasks = new List<Task>(poolsByHosts.Count);
                foreach (var poolKvp in poolsByHosts)
                {
                    if (poolKvp.Key.Address.Equals(prepareResult.HostAddress))
                    {
                        continue;
                    }

                    if (prepareResult.TriedHosts.ContainsKey(poolKvp.Key.Address))
                    {
                        PrepareHandler.Logger.Warning(
                            $"An error occured while attempting to prepare query on {{0}}:{Environment.NewLine}{{1}}",
                            poolKvp.Key.Address,
                            prepareResult.TriedHosts[poolKvp.Key.Address]);
                        continue;
                    }

                    await semaphore.WaitAsync().ConfigureAwait(false);
                    tasks.Add(ReprepareOnSingleNodeAsync(observer, sessionRequestInfo, poolKvp, prepareResult.PreparedStatement, request, semaphore, false));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        private static Task<IConnection> GetConnectionFromHostAsync(
            IHostConnectionPool pool, PreparedStatement ps, IDictionary<IPEndPoint, Exception> triedHosts)
        {
            return GetConnectionFromHostInternalAsync(pool, ps, triedHosts, true);
        }

        private static async Task<IConnection> GetConnectionFromHostInternalAsync(
            IHostConnectionPool pool, PreparedStatement ps, IDictionary<IPEndPoint, Exception> triedHosts, bool retry)
        {
            try
            {
                return await pool.GetExistingConnectionFromHostAsync(triedHosts, () => ps.Keyspace, ps.RoutingKey, -1).ConfigureAwait(false);
            }
            catch (SocketException)
            {
                if (retry)
                {
                    // A socket exception on the current connection does not mean that all the pool is closed:
                    // Retry on the same pool
                    return await ReprepareHandler.GetConnectionFromHostInternalAsync(pool, ps, triedHosts, false).ConfigureAwait(false);
                }

                throw;
            }
        }

        public Task ReprepareOnSingleNodeAsync(
            KeyValuePair<Host, IHostConnectionPool> poolKvp, PreparedStatement ps, IRequest request, SemaphoreSlim sem, bool throwException)
        {
            return ReprepareOnSingleNodeAsync(null, null, poolKvp, ps, request, sem, throwException);
        }

        public async Task ReprepareOnSingleNodeAsync(
            IRequestObserver observer,
            SessionRequestInfo sessionRequestInfo,
            KeyValuePair<Host, IHostConnectionPool> poolKvp,
            PreparedStatement ps,
            IRequest request,
            SemaphoreSlim sem,
            bool throwException)
        {
            NodeRequestInfo nodeRequestInfo = null;
            if (observer != null)
            {
                nodeRequestInfo = new NodeRequestInfo(poolKvp.Key, sessionRequestInfo.PrepareRequest ?? new PrepareRequest(ps.Cql, ps.Keyspace));
                await observer.OnNodeStartAsync(sessionRequestInfo, nodeRequestInfo).ConfigureAwait(false);
            }

            try
            {
                var triedHosts = new Dictionary<IPEndPoint, Exception>();
                var connection = await ReprepareHandler.GetConnectionFromHostAsync(poolKvp.Value, ps, triedHosts).ConfigureAwait(false);

                if (connection != null)
                {
                    await connection.Send(request).ConfigureAwait(false);
                    if (observer != null)
                    {
                        await observer.OnNodeSuccessAsync(sessionRequestInfo, nodeRequestInfo).ConfigureAwait(false);
                    }
                    return;
                }

                if (triedHosts.TryGetValue(poolKvp.Key.Address, out var ex))
                {
                    LogOrThrow(
                        throwException,
                        ex,
                        $"An error occured while attempting to prepare query on {{0}}:{Environment.NewLine}{{1}}",
                        poolKvp.Key,
                        ex);
                    if (observer != null)
                    {
                        await observer.OnNodeRequestErrorAsync(
                            RequestError.CreateServerError(ex),
                            sessionRequestInfo,
                            nodeRequestInfo).ConfigureAwait(false);
                    }
                    return;
                }

                LogOrThrow(
                    throwException,
                    null,
                    "Could not obtain an existing connection to prepare query on {0}.",
                    poolKvp.Key);
                if (observer != null)
                {
                    await observer.OnNodeRequestErrorAsync(
                        RequestError.CreateClientError(new DriverInternalError($"Could not obtain an existing connection to prepare query on {poolKvp.Key}."), false),
                        sessionRequestInfo,
                        nodeRequestInfo).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                if (observer != null)
                {
                    await observer.OnNodeRequestErrorAsync(
                        RequestError.CreateServerError(ex),
                        sessionRequestInfo,
                        nodeRequestInfo).ConfigureAwait(false);
                }
                LogOrThrow(
                    throwException,
                    ex,
                    $"An error occured while attempting to prepare query on {{0}}:{Environment.NewLine}{{1}}",
                    poolKvp.Key,
                    ex);
            }
            finally
            {
                sem.Release();
            }
        }

        private void LogOrThrow(bool throwException, Exception ex, string msg, params object[] args)
        {
            if (throwException)
            {
                if (ex == null)
                {
                    throw new Exception(string.Format(msg, args));
                }

                throw ex;
            }

            PrepareHandler.Logger.Warning(msg, args);
        }
    }
}