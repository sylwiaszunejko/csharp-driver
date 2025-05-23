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

using System.Threading.Tasks;
using Cassandra.Serialization;

namespace Cassandra.Connections.Control
{
    internal interface IProtocolVersionNegotiator
    {
        Task<IConnection> ChangeProtocolVersion(Configuration config, ISerializerManager serializer, ProtocolVersion nextVersion, IConnection previousConnection, UnsupportedProtocolVersionException ex = null, ProtocolVersion? previousVersion = null);

        Task<IConnection> NegotiateVersionAsync(Configuration config, Metadata metadata, IConnection connection, ISerializerManager serializer);
    }
}