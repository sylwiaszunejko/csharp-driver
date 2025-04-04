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
using Cassandra.Metrics;
using Cassandra.Metrics.Providers.Null;
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Metrics;
using Cassandra.Responses;
using Cassandra.Tasks;

namespace Cassandra.Tests
{
    internal static class OperationStateExtensions
    {
        public static OperationState CreateMock(Action<Exception, Response> action)
        {
            return new OperationState(
                (error, response) =>
                {
                    action(error?.Exception, response);
                    return TaskHelper.Completed;
                },
                null,
                0,
                new MetricsOperationObserver(new NodeMetrics(new NullDriverMetricsProvider(), new DriverMetricsOptions(), false, "c"), false));
        }
    }
}