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

using System.Collections;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Reflection;
using Cassandra.Serialization;
using Cassandra.Serialization.Primitive;

namespace Cassandra.Tests
{
    [TestFixture]
    public class SerializerTests
    {
        private readonly ProtocolVersion[] _protocolVersions =
        {
            ProtocolVersion.V1, ProtocolVersion.V2, ProtocolVersion.V3, ProtocolVersion.V4
        };

        private static readonly MapColumnInfo MapColumnInfoStringString = new MapColumnInfo() { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Text };

        [Test]
        public void EncodeDecodeSingleValuesTest()
        {
            var initialValues = new object[]
            {
                "utf8 text mañana",
                1234,
                3129L,
                1234F,
                1.14D,
                double.MinValue,
                float.MinValue,
                -1.14,
                0d,
                double.MaxValue,
                float.MaxValue,
                double.NaN,
                float.NaN,
                1.01M,
                72.727272727272727272727272727M,
                -72.727272727272727272727272727M,
                -256M,
                256M,
                0M,
                -1.333333M,
                -256.512M,
                Decimal.MaxValue,
                Decimal.MinValue,
                new DateTimeOffset(new DateTime(2015, 10, 21)),
                new IPAddress(new byte[] { 1, 1, 5, 255}),
                true,
                new byte[] {16},
                Guid.NewGuid(),
                Guid.NewGuid(),
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (var value in initialValues)
                {
                    byte[] encoded = serializer.Serialize(value);
                    Assert.AreEqual(value, serializer.Deserialize(encoded, serializer.GetCqlTypeForPrimitive(value.GetType()), null));
                }
            }
        }

        /// <summary>
        /// Tests that the default target type when is not provided
        /// </summary>
        [Test]
        [TestCaseSource(nameof(SingleValuesTestCases))]
        public void EncodeDecodeSingleValuesDefaultsFactory(object[] feed)
        {
            var value = ((Array)feed[0]).GetValue(0);
            var code = (ColumnTypeCode)feed[1];
            foreach (var protocolVersion in _protocolVersions)
            {
                var serializer = NewInstance(protocolVersion);
                byte[] encoded = serializer.Serialize(value);
                //Set object as the target CSharp type, it should get the default value
                Assert.AreEqual(value, serializer.Deserialize(encoded, code, null));
            }
        }

        static IEnumerable<object[]> SingleValuesTestCases()
        {
            // 2-element array, type code
            yield return new object[] { new[] { "just utf8 text olé!", "another" }, ColumnTypeCode.Text };
            yield return new object[] { new[] { 123, -1 }, ColumnTypeCode.Int };
            yield return new object[] { new[] { Int64.MinValue + 100, 1 }, ColumnTypeCode.Bigint };
            yield return new object[] { new[] { -144F, 1.25F }, ColumnTypeCode.Float };
            yield return new object[] { new[] { 1120D, 1.3456D }, ColumnTypeCode.Double };
            yield return new object[] { new[] { -9999.89770M, 8.0923M }, ColumnTypeCode.Decimal };
            yield return new object[] { new[] { new DateTimeOffset(new DateTime(2010, 4, 29)), new DateTimeOffset(new DateTime(1980, 1, 9)) }, ColumnTypeCode.Timestamp };
            yield return new object[] { new[] { new IPAddress(new byte[] { 10, 0, 5, 5 }), new IPAddress(new byte[] { 127, 0, 0, 1 }) }, ColumnTypeCode.Inet };
            yield return new object[] { new[] { Guid.NewGuid(), Guid.NewGuid() }, ColumnTypeCode.Uuid };
            yield return new object[] { new[] { true, false }, ColumnTypeCode.Boolean };
            yield return new object[] { new[] { new byte[] { 255, 128, 64, 32, 16, 9, 9 }, new byte[] { 0, 1, 128, 9, 1, 2, 3, 4 } }, ColumnTypeCode.Blob };
            yield return new object[] { new[] { TimeUuid.NewId().ToGuid(), TimeUuid.NewId().ToGuid() }, ColumnTypeCode.Timeuuid };
            yield return new object[] { new sbyte[] { 0, 1 }, ColumnTypeCode.TinyInt };
            yield return new object[] { new short[] { -1, 1 }, ColumnTypeCode.SmallInt };
            yield return new object[] { new[] { BigInteger.Parse("10000000"), BigInteger.One }, ColumnTypeCode.Varint };
            yield return new object[] { new[] { Duration.Zero, Duration.Parse("1y2mo") }, ColumnTypeCode.Duration };
            yield return new object[] { new[] { new LocalDate(2020, 1, 2), new LocalDate(1970, 12, 12) }, ColumnTypeCode.Date };
            yield return new object[] { new[] { new LocalTime(1, 2, 3, 4), new LocalTime(23, 59, 59, 999) }, ColumnTypeCode.Time };
        }

        static IEnumerable<object[]> CollectionsTestCases()
        {
            // value, type code, column info
            foreach (object[] row in SingleValuesTestCases())
            {
                // List
                yield return new object[]
                    { row[0], ColumnTypeCode.List, new ListColumnInfo() { ValueTypeCode = (ColumnTypeCode)row[1] } };
                IEnumerable<object> list = ((Array)row[0]).Cast<object>().ToList();

                yield return new object[]
                {
                    new List<object>(list), ColumnTypeCode.List,
                    new ListColumnInfo() { ValueTypeCode = (ColumnTypeCode)row[1], ValueTypeInfo = new ListColumnInfo(){}}
                };

                // Set
                yield return new object[]
                {
                    new List<object>(list), ColumnTypeCode.Set,
                    new SetColumnInfo() { KeyTypeCode = (ColumnTypeCode)row[1] }
                };
                yield return new object[]
                {
                    new HashSet<object>(list), ColumnTypeCode.Set,
                    new SetColumnInfo() { KeyTypeCode = (ColumnTypeCode)row[1] }
                };
                yield return new object[]
                {
                    row[0], ColumnTypeCode.Set,
                    new SetColumnInfo() { KeyTypeCode = (ColumnTypeCode)row[1] }
                };

                // Vector
                yield return new object[]
                {
                    CreateCqlVectorDynamicType((Array)row[0]), ColumnTypeCode.Custom,
                    new VectorColumnInfo() { ValueTypeCode = (ColumnTypeCode)row[1], Dimensions = 2}
                };
            }
        }

        static IEnumerable<object[]> VectorsTestCases()
        {
            // value, type code, column info
            foreach (object[] singleValueRow in SingleValuesTestCases())
            {
                yield return new[]
                {
                    CreateCqlVectorDynamicType((Array)singleValueRow[0]), ColumnTypeCode.Custom,
                    new VectorColumnInfo() { ValueTypeCode = (ColumnTypeCode)singleValueRow[1], Dimensions = 2}
                };
            }

            foreach (object[] collectionRow in CollectionsTestCases())
            {
                Type subtype = collectionRow[0].GetType();
                Type cqlVectorType = typeof(CqlVector<>).MakeGenericType(subtype);
                object[] param = new[] { collectionRow[0], collectionRow[0] };
                object vector = Activator.CreateInstance(cqlVectorType, param);
                yield return new[]
                {
                    vector, ColumnTypeCode.Custom,
                    new VectorColumnInfo() { ValueTypeCode = (ColumnTypeCode)collectionRow[1], Dimensions = 2, ValueTypeInfo = (IColumnInfo) collectionRow[2] }
                };
            }
        }

        static object CreateCqlVectorDynamicType(Array array)
        {
            Type elementType = array.GetType().GetElementType();
            Type cqlVectorType = typeof(CqlVector<>).MakeGenericType(elementType);
            ConstructorInfo constructor = cqlVectorType.GetConstructor(new Type[] { array.GetType() });
            return constructor.Invoke(new object[] { array });
        }

        [Test]
        [TestCaseSource(nameof(CollectionsTestCases))]
        public void EncodeDecodeListSetFactoryTest(object[] feed)
        {
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                var valueToEncode = (IEnumerable)feed[0];
                var encoded = serializer.Serialize(valueToEncode);
                var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)feed[1], (IColumnInfo)feed[2]);
                CollectionAssert.AreEqual(valueToEncode, decoded);
            }
        }

        [Test]
        [TestCaseSource(nameof(VectorsTestCases))]
        public void EncodeDecodeVectorFactoryTest(object[] feed)
        {
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                var valueToEncode = (IEnumerable)feed[0];
                var encoded = serializer.Serialize(valueToEncode);
                var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)feed[1], (IColumnInfo)feed[2]);
                CollectionAssert.AreEqual(valueToEncode, decoded);
            }
        }

        [Test]
        public void EncodeListSetInvalid()
        {
            var values = new object[]
            {
                //any class that is not a valid primitive
                new List<object> { new object()},
                new List<Action> {() => { }}
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (var value in values)
                {
                    Assert.Throws<InvalidTypeException>(() => serializer.Serialize(value));
                }
            }
        }

        [Test]
        public void EncodeDecodeMapFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[] {new SortedDictionary<string, string>(), ColumnTypeCode.Map, MapColumnInfoStringString},
                new object[] {new SortedDictionary<string, string>{{"key100","value100"}}, ColumnTypeCode.Map, MapColumnInfoStringString},
                new object[] {new SortedDictionary<string, string>{{"key1","value1"}, {"key2","value2"}}, ColumnTypeCode.Map, MapColumnInfoStringString},
                new object[] {new SortedDictionary<string, int>{{"key1", 1}, {"key2", 2}}, ColumnTypeCode.Map, new MapColumnInfo() {KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Int}},
                new object[] {new SortedDictionary<Guid, string>{{Guid.NewGuid(),"value1"}, {Guid.NewGuid(),"value2"}}, ColumnTypeCode.Map, new MapColumnInfo() {KeyTypeCode = ColumnTypeCode.Uuid, ValueTypeCode = ColumnTypeCode.Text}},
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var valueToEncode = (IDictionary)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IDictionary)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    CollectionAssert.AreEquivalent(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void EncodeDecodeTupleFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[] {new Tuple<string>("val1"), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}}}},
                new object[] {new Tuple<string, int>("val2", 2), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}, new ColumnDesc(){TypeCode = ColumnTypeCode.Int}}}},
                new object[] {new Tuple<string, int>(null, -1234), ColumnTypeCode.Tuple, new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}, new ColumnDesc(){TypeCode = ColumnTypeCode.Int}}}}
            };
            var serializer = NewInstance();
            foreach (object[] value in initialValues)
            {
                var valueToEncode = (IStructuralEquatable)value[0];
                var encoded = serializer.Serialize(valueToEncode);
                var decoded = (IStructuralEquatable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                Assert.AreEqual(valueToEncode, decoded);
            }
        }

        [Test]
        public void EncodeDecodeTupleAsSubtypeFactoryTest()
        {
            var initialValues = new object[]
            {
                new object[]
                {
                    new List<Tuple<string>>{new Tuple<string>("val1")},
                    ColumnTypeCode.List,
                    new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Tuple, ValueTypeInfo = new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}}}}
                },
                new object[]
                {
                    new List<Tuple<string, int>>{new Tuple<string, int>("val2ZZ", 0)},
                    ColumnTypeCode.List,
                    new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Tuple, ValueTypeInfo = new TupleColumnInfo() { Elements = new List<ColumnDesc>() {new ColumnDesc(){TypeCode = ColumnTypeCode.Text}, new ColumnDesc(){TypeCode = ColumnTypeCode.Int}}}}
                }
            };
            var serializer = NewInstance();
            foreach (object[] value in initialValues)
            {
                var valueToEncode = (IList)value[0];
                var encoded = serializer.Serialize(valueToEncode);
                var decoded = (IList)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                Assert.AreEqual(valueToEncode, decoded);
            }
        }

        [Test]
        public void Encode_Decode_Nested_List()
        {
            var initialValues = new object[]
            {
                new object[] {new IEnumerable<int>[]{new List<int>(new [] {1, 2, 1000})}, ColumnTypeCode.List, GetNestedListColumnInfo(1, ColumnTypeCode.Int)},
                new object[] {new IEnumerable<IEnumerable<int>>[]{new List<IEnumerable<int>> {new List<int>(new [] {1, 2, 1000})}}, ColumnTypeCode.List, GetNestedListColumnInfo(2, ColumnTypeCode.Int)}
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var originalType = value[0].GetType();
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    Assert.IsInstanceOf(originalType, decoded);
                    CollectionAssert.AreEqual(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void Encode_Decode_Nested_Set()
        {
            var initialValues = new object[]
            {
                new object[] {new SortedSet<IEnumerable<int>>{new SortedSet<int>(new [] {1, 2, 1000})}, ColumnTypeCode.Set, GetNestedSetColumnInfo(1, ColumnTypeCode.Int)},
                new object[] {new SortedSet<IEnumerable<IEnumerable<int>>>{new SortedSet<IEnumerable<int>> {new SortedSet<int>(new [] {1, 2, 1000})}}, ColumnTypeCode.Set, GetNestedSetColumnInfo(2, ColumnTypeCode.Int)}
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    //The return type is not respected
                    CollectionAssert.AreEqual(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void Encode_Decode_Nested_Map()
        {
            var initialValues = new object[]
            {
                new object[] {
                    new SortedDictionary<string, IEnumerable<int>>{{"first", new List<int>(new [] {1, 2, 1000})}},
                    ColumnTypeCode.Map,
                    new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.List, ValueTypeInfo = new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Int}}
                },
                new object[] {
                    new SortedDictionary<int, IEnumerable<string>>{{120, new SortedSet<string>(new [] {"a", "b", "c"})}},
                    ColumnTypeCode.Map,
                    new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Int, ValueTypeCode = ColumnTypeCode.Set, ValueTypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Text}}
                },
                new object[] {
                    new SortedDictionary<string, IDictionary<string, int>>{{"first-b", new SortedDictionary<string, int> {{"A", 1}, {"B", 2}}}},
                    ColumnTypeCode.Map,
                    new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Map, ValueTypeInfo = new MapColumnInfo{ KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Int}}
                }
            };
            foreach (var version in _protocolVersions)
            {
                var serializer = NewInstance(version);
                foreach (object[] value in initialValues)
                {
                    var originalType = value[0].GetType();
                    var valueToEncode = (IEnumerable)value[0];
                    var encoded = serializer.Serialize(valueToEncode);
                    var decoded = (IEnumerable)serializer.Deserialize(encoded, (ColumnTypeCode)value[1], (IColumnInfo)value[2]);
                    Assert.IsInstanceOf(originalType, decoded);
                    CollectionAssert.AreEqual(valueToEncode, decoded);
                }
            }
        }

        [Test]
        public void Encode_Decode_TinyInt()
        {
            var values = new[]
            {
                Tuple.Create<sbyte, byte>(-1, 0xff),
                Tuple.Create<sbyte, byte>(-2, 0xfe),
                Tuple.Create<sbyte, byte>(0, 0),
                Tuple.Create<sbyte, byte>(1, 1),
                Tuple.Create<sbyte, byte>(2, 2),
                Tuple.Create<sbyte, byte>(127, 127)
            };
            var serializer = NewInstance();
            foreach (var v in values)
            {
                var encoded = serializer.Serialize(v.Item1);
                CollectionAssert.AreEqual(encoded, new[] { v.Item2 });
                var decoded = (sbyte)serializer.Deserialize(encoded, ColumnTypeCode.TinyInt, null);
                Assert.AreEqual(v.Item1, decoded);
            }
        }

        [Test]
        public void Encode_Decode_Date()
        {
            var values = new[]
            {
                new LocalDate(2010, 4, 29),
                new LocalDate(2005, 8, 5),
                new LocalDate(0, 3, 12),
                new LocalDate(-10, 2, 4),
                new LocalDate(5881580, 7, 11),
                new LocalDate(-5877641, 6, 23)
            };
            var serializer = NewInstance();
            foreach (var v in values)
            {
                var encoded = serializer.Serialize(v);
                var decoded = (LocalDate)serializer.Deserialize(encoded, ColumnTypeCode.Date, null);
                Assert.AreEqual(v, decoded);
            }
        }

        [Test]
        public void Encode_Decode_SmallInt()
        {
            var serializer = NewInstance();
            for (var i = Int16.MinValue; ; i++)
            {
                var encoded = serializer.Serialize(i);
                var decoded = (short)serializer.Deserialize(encoded, ColumnTypeCode.SmallInt, null);
                Assert.AreEqual(i, decoded);
                if (i == Int16.MaxValue)
                {
                    break;
                }
            }
        }

        [Test]
        public void Encode_Map_With_Null_Value_Throws_ArgumentNullException()
        {
            var value = new Dictionary<string, string>
            {
                {"k1", "value1"},
                {"k2", null}
            };
            var serializer = NewInstance();
            //null value within a dictionary
            var ex = Assert.Throws<ArgumentNullException>(() => serializer.Serialize(value));
            StringAssert.Contains("collections", ex.Message);
        }

        [Test]
        public void Encode_List_With_Null_Value_Throws_ArgumentNullException()
        {
            var value = new List<string>
            {
                "one",
                null,
                "two"
            };
            var serializer = NewInstance();
            //null value within a list
            var ex = Assert.Throws<ArgumentNullException>(() => serializer.Serialize(value));
            StringAssert.Contains("collections", ex.Message);
        }

        [Test]
        public void Encode_Decode_With_Binary_Representation()
        {
            var values = new[]
            {
                Tuple.Create<object, byte[]>(1D, new byte[] {0x3f, 0xf0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create<object, byte[]>(2D, new byte[] {0x40, 0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create<object, byte[]>(2.2D, new byte[] {0x40, 1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a}),
                Tuple.Create<object, byte[]>(-1D, new byte[] {0xbf, 0xf0, 0, 0, 0, 0, 0, 0}),
                Tuple.Create<object, byte[]>(-1F, new byte[] {0xbf, 0x80, 0, 0}),
                Tuple.Create<object, byte[]>(1.3329F, new byte[] {0x3f, 0xaa, 0x9c, 0x78}),
                Tuple.Create<object, byte[]>("abc", new byte[] {0x61, 0x62, 0x63})
            };
            var serializer = NewInstance();
            foreach (var val in values)
            {
                var encoded = serializer.Serialize(val.Item1);
                CollectionAssert.AreEqual(val.Item2, encoded);
                var padEncoded = new byte[] { 0xFF, 0xFA }.Concat(encoded).ToArray();
                Assert.AreEqual(val.Item1, serializer.Deserialize(padEncoded, 2, encoded.Length, serializer.GetCqlTypeForPrimitive(val.Item1.GetType()), null));
            }
        }

        [Test]
        public void GetClrType_Should_Get_Clr_Type_For_Primitive_Cql_Types()
        {
            var notPrimitive = new[] { ColumnTypeCode.List, ColumnTypeCode.Set, ColumnTypeCode.Map, ColumnTypeCode.Udt, ColumnTypeCode.Tuple, ColumnTypeCode.Custom };
            var serializer = NewInstance();
            foreach (ColumnTypeCode typeCode in Enum.GetValues(typeof(ColumnTypeCode)))
            {
                if (notPrimitive.Contains(typeCode))
                {
                    continue;
                }
                var type = serializer.GetClrType(typeCode, null);
                Assert.NotNull(type);
                if (type.GetTypeInfo().IsValueType)
                {
                    Assert.NotNull(serializer.Serialize(Activator.CreateInstance(type)));
                }
            }
        }

        [Test]
        public void GetClrType_Should_Get_Clr_Type_For_Non_Primitive_Cql_Types()
        {
            var notPrimitive = new[]
            {
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(IEnumerable<string>), ColumnTypeCode.List, new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Text}),
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(IEnumerable<int>), ColumnTypeCode.Set, new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Int}),
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(IEnumerable<IEnumerable<DateTimeOffset>>), ColumnTypeCode.List,
                    new ListColumnInfo { ValueTypeCode = ColumnTypeCode.Set, ValueTypeInfo = new SetColumnInfo { KeyTypeCode = ColumnTypeCode.Timestamp}}),
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(IDictionary<string, int>), ColumnTypeCode.Map,
                    new MapColumnInfo { KeyTypeCode = ColumnTypeCode.Text, ValueTypeCode = ColumnTypeCode.Int }),
                Tuple.Create<Type, ColumnTypeCode, IColumnInfo>(typeof(Tuple<string, int, LocalDate>), ColumnTypeCode.Tuple,
                    new TupleColumnInfo(new [] { ColumnTypeCode.Text, ColumnTypeCode.Int, ColumnTypeCode.Date}.Select(c => new ColumnDesc {TypeCode = c})))
            };
            var serializer = NewInstance();
            foreach (var item in notPrimitive)
            {
                var type = serializer.GetClrType(item.Item2, item.Item3);
                Assert.AreEqual(item.Item1, type);
            }
        }

        [Test]
        public void DecimalSerializer_ToDecimal_Converts_Test()
        {
            var values = new[]
            {
                Tuple.Create(BigInteger.Parse("1000"), 1, 100M),
                Tuple.Create(BigInteger.Parse("1000"), 0, 1000M),
                Tuple.Create(BigInteger.Parse("9223372036854776"), -1, 92233720368547760M),
                Tuple.Create(BigInteger.Parse("12345678901234567890"), 2, 123456789012345678.9M),
                Tuple.Create(BigInteger.Parse("79228162514264337593543950335"), 0, 79228162514264337593543950335M),
                Tuple.Create(BigInteger.Parse("79228162514264337593543950335"), 27, 79.228162514264337593543950335M),
                Tuple.Create(BigInteger.Parse("1"), -28, 10000000000000000000000000000m)
            };
            foreach (var v in values)
            {
                var decimalValue = DecimalSerializer.ToDecimal(v.Item1, v.Item2);
                Assert.AreEqual(v.Item3, decimalValue);
            }
        }

        [Test]
        public void DecimalSerializer_ToDecimal_Throws_OverflowException_When_Value_Can_Not_Be_Represented_Test()
        {
            var values = new[]
            {
                Tuple.Create(BigInteger.Parse("123"), -28),
                Tuple.Create(BigInteger.Parse("123"), -27),
                Tuple.Create(BigInteger.Parse("1"), 29),
                Tuple.Create(BigInteger.Parse("123456789012345678901234567890"), 0)
            };
            foreach (var v in values)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => DecimalSerializer.ToDecimal(v.Item1, v.Item2), "For value: " + v.Item1);
            }
        }

        private static ISerializer NewInstance(ProtocolVersion protocolVersion = ProtocolVersion.MaxSupported)
        {
            return new SerializerManager(protocolVersion).GetCurrentSerializer();
        }

        /// <summary>
        /// Helper method to generate a list column info of nested lists
        /// </summary>
        private static ListColumnInfo GetNestedListColumnInfo(int level, ColumnTypeCode singleType)
        {
            var columnInfo = new ListColumnInfo();
            if (level == 0)
            {
                columnInfo.ValueTypeCode = singleType;
                columnInfo.ValueTypeInfo = null;
            }
            else
            {
                columnInfo.ValueTypeCode = ColumnTypeCode.List;
                columnInfo.ValueTypeInfo = GetNestedListColumnInfo(level - 1, singleType);
            }
            return columnInfo;
        }

        /// <summary>
        /// Helper method to generate a set column info of nested sets
        /// </summary>
        private static SetColumnInfo GetNestedSetColumnInfo(int level, ColumnTypeCode singleType)
        {
            var columnInfo = new SetColumnInfo();
            if (level == 0)
            {
                columnInfo.KeyTypeCode = singleType;
                columnInfo.KeyTypeInfo = null;
            }
            else
            {
                columnInfo.KeyTypeCode = ColumnTypeCode.Set;
                columnInfo.KeyTypeInfo = GetNestedSetColumnInfo(level - 1, singleType);
            }
            return columnInfo;
        }
    }

    public static class SerializedExtensions
    {
        internal static object Deserialize(this ISerializer serializer, byte[] buffer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return serializer.Deserialize(buffer, 0, buffer.Length, typeCode, typeInfo);
        }

        internal static ColumnTypeCode GetCqlTypeForPrimitive(this IGenericSerializer serializer, Type type)
        {
            return serializer.GetCqlType(type, out IColumnInfo dummyInfo);
        }
    }
}
