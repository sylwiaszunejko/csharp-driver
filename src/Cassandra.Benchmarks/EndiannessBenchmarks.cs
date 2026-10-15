//
//      Copyright (C) ScyllaDB
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
using BenchmarkDotNet.Attributes;

namespace Cassandra.Benchmarks
{
    /// <summary>
    /// Big-endian primitive conversions used by every frame and every fixed-size value, plus the
    /// <see cref="TimeUuid"/> paths that reorder bytes by hand.
    /// </summary>
    [MemoryDiagnoser]
    public class EndiannessBenchmarks
    {
        private readonly byte[] _int32Bytes = BeConverter.GetBytes(123456789);
        private readonly byte[] _int64Bytes = BeConverter.GetBytes(1234567890123456789L);
        private readonly byte[] _doubleBytes = BeConverter.GetBytes(Math.PI);
        private readonly byte[] _singleBytes = BeConverter.GetBytes((float)Math.E);
        private readonly DateTimeOffset _timestamp = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
        private readonly TimeUuid _timeUuid = TimeUuid.NewId(new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero));

        [Benchmark]
        public byte[] Int32ToBytes() => BeConverter.GetBytes(123456789);

        [Benchmark]
        public int Int32FromBytes() => BeConverter.ToInt32(_int32Bytes);

        [Benchmark]
        public byte[] Int64ToBytes() => BeConverter.GetBytes(1234567890123456789L);

        [Benchmark]
        public long Int64FromBytes() => BeConverter.ToInt64(_int64Bytes);

        [Benchmark]
        public byte[] DoubleToBytes() => BeConverter.GetBytes(Math.PI);

        [Benchmark]
        public double DoubleFromBytes() => BeConverter.ToDouble(_doubleBytes);

        [Benchmark]
        public float SingleFromBytes() => BeConverter.ToSingle(_singleBytes);

        [Benchmark]
        public TimeUuid TimeUuidNewId() => TimeUuid.NewId(_timestamp);

        [Benchmark]
        public DateTimeOffset TimeUuidGetDate() => _timeUuid.GetDate();
    }
}
