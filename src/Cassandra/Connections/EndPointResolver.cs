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
using System.Net;
using System.Threading.Tasks;

namespace Cassandra.Connections
{
    internal class EndPointResolver : IEndPointResolver
    {
        private readonly IServerNameResolver _serverNameResolver;

        public EndPointResolver(IServerNameResolver serverNameResolver)
        {
            _serverNameResolver = serverNameResolver ?? throw new ArgumentNullException(nameof(serverNameResolver));
        }

        /// <inheritdoc />
        public Task<IConnectionEndPoint> GetConnectionShardAwareEndPointAsync(Host host, bool refreshCache, int shardAwarePort)
        {
            return Task.FromResult((IConnectionEndPoint)new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse(host.Address.ToString().Split(':')[0]), shardAwarePort), _serverNameResolver, host.ContactPoint));
        }

        /// <inheritdoc />
        public Task<IConnectionEndPoint> GetConnectionEndPointAsync(Host host, bool refreshCache)
        {
            return Task.FromResult((IConnectionEndPoint)new ConnectionEndPoint(host.Address, _serverNameResolver, host.ContactPoint));
        }
    }
}