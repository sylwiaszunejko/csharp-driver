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
using Cassandra.Requests;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// <para>Represents a prepared statement with the parameter values set, ready for execution.</para>
    /// A <see cref="BoundStatement"/> can be created from a <see cref="PreparedStatement"/> instance using the
    /// <c>Bind()</c> method and can be executed using a <see cref="ISession"/> instance.
    /// <seealso cref="PreparedStatement"/>
    /// </summary>
    public class BoundStatement : Statement
    {
        private static readonly Logger Logger = new Logger(typeof(BoundStatement));
        private readonly PreparedStatement _preparedStatement;
        private RoutingKey _routingKey;
        private readonly string _keyspace;
        private readonly string _table;

        /// <summary>
        ///  Gets the prepared statement on which this BoundStatement is based.
        /// </summary>
        public PreparedStatement PreparedStatement
        {
            get { return _preparedStatement; }
        }


        /// <summary>
        ///  Gets the routing key for this bound query. <p> This method will return a
        ///  non-<c>null</c> value if: <ul> <li>either all the TableColumns composing the
        ///  partition key are bound variables of this <c>BoundStatement</c>. The
        ///  routing key will then be built using the values provided for these partition
        ///  key TableColumns.</li> <li>or the routing key has been set through
        ///  <c>PreparedStatement.SetRoutingKey</c> for the
        ///  <see cref="PreparedStatement"/> this statement has been built from.</li> </ul>
        ///  Otherwise, <c>null</c> is returned.</p> <p> Note that if the routing key
        ///  has been set through <link>PreparedStatement.SetRoutingKey</link>, that value
        ///  takes precedence even if the partition key is part of the bound variables.</p>
        /// </summary>
        public override RoutingKey RoutingKey
        {
            get { return _routingKey; }
        }

        /// <summary>
        /// Returns the keyspace this query operates on, based on the <see cref="PreparedStatement"/> metadata.
        /// <para>
        /// The keyspace returned is used as a hint for token-aware routing.
        /// </para>
        /// </summary>
        public override string Keyspace
        {
            get { return _keyspace; }
        }

        public override string TableName
        {
            get { return _table; }
        }

        public override bool IsLwt()
        {
            return _preparedStatement.IsLwt;
        }

        public BoundStatement SetLwt(bool isLwt)
        {
            _preparedStatement.SetLwt(isLwt);
            return this;
        }

        /// <summary>
        /// Initializes a new instance of the Cassandra.BoundStatement class
        /// </summary>
        public BoundStatement()
        {
            //Default constructor for client test and mocking frameworks
        }

        /// <summary>
        ///  Creates a new <c>BoundStatement</c> from the provided prepared
        ///  statement.
        /// </summary>
        /// <param name="statement"> the prepared statement from which to create a <c>BoundStatement</c>.</param>
        public BoundStatement(PreparedStatement statement)
        {
            _preparedStatement = statement;
            _routingKey = statement.RoutingKey;
            _keyspace = statement.Keyspace ?? statement.Variables?.Keyspace;
            _table = statement.Variables?.Table;

            SetConsistencyLevel(statement.ConsistencyLevel);
            if (statement.IsIdempotent != null)
            {
                SetIdempotence(statement.IsIdempotent.Value);
            }
        }

        /// <summary>
        ///  Set the routing key for this query. This method allows to manually
        ///  provide a routing key for this BoundStatement. It is thus optional since the routing
        ///  key is only an hint for token aware load balancing policy but is never
        ///  mandatory.
        /// </summary>
        /// <param name="routingKeyComponents"> the raw (binary) values to compose the routing key.</param>
        public BoundStatement SetRoutingKey(params RoutingKey[] routingKeyComponents)
        {
            _routingKey = RoutingKey.Compose(routingKeyComponents);
            return this;
        }

        internal override void SetValues(object[] values, ISerializer serializer)
        {
            values = ValidateValues(values, serializer);
            base.SetValues(values, serializer);
        }

        /// <summary>
        /// Validate values using prepared statement metadata,
        /// returning a new instance of values to be used as parameters.
        /// </summary>
        private object[] ValidateValues(object[] values, ISerializer serializer)
        {
            if (serializer == null)
            {
                throw new DriverInternalError("Serializer can not be null");
            }

            if (values == null)
            {
                return null;
            }
            if (PreparedStatement.Variables == null || PreparedStatement.Variables.Columns == null || PreparedStatement.Variables.Columns.Length == 0)
            {
                return values;
            }
            var paramsMetadata = PreparedStatement.Variables.Columns;
            if (values.Length > paramsMetadata.Length)
            {
                throw new ArgumentException(
                    string.Format("Provided {0} parameters to bind, expected {1}", values.Length, paramsMetadata.Length));
            }
            for (var i = 0; i < values.Length; i++)
            {
                var p = paramsMetadata[i];
                var value = values[i];
                bool assignable;
                string failureMsg;
                if (serializer.IsEncryptionEnabled)
                {
                    var tuple = serializer.IsAssignableFromEncrypted(p.Keyspace ?? Keyspace, p.Table, p.Name, p.TypeCode, p.TypeInfo, value, out failureMsg);
                    assignable = tuple.Item1;
                }
                else
                {
                    assignable = serializer.IsAssignableFrom(p.TypeCode, p.TypeInfo, value, out failureMsg);
                }

                if (!assignable)
                {
                    throw new InvalidTypeException(failureMsg);
                }
            }
            if (values.Length < paramsMetadata.Length && serializer.ProtocolVersion.SupportsUnset())
            {
                //Set the result of the unspecified parameters to Unset
                var completeValues = new object[paramsMetadata.Length];
                values.CopyTo(completeValues, 0);
                for (var i = values.Length; i < paramsMetadata.Length; i++)
                {
                    completeValues[i] = Unset.Value;
                }
                values = completeValues;
            }
            return values;
        }

        internal override IQueryRequest CreateBatchRequest(ISerializer serializer)
        {
            // Use the default query options as the individual options of the query will be ignored
            var options = QueryProtocolOptions.CreateForBatchItem(this, PreparedStatement.Variables);
            return new ExecuteRequest(
                serializer,
                PreparedStatement.Id,
                PreparedStatement.ResultMetadata,
                options,
                IsTracing,
                null,
                true);
        }

        internal void CalculateRoutingKey(
            ISerializer serializer,
            bool useNamedParameters,
            int[] routingIndexes,
            string[] routingNames,
            object[] valuesByPosition,
            object[] rawValues)
        {
            if (_routingKey != null)
            {
                //The routing key was specified by the user
                return;
            }

            var parametersMetadata = PreparedStatement.Variables;
            if (routingIndexes != null)
            {
                if (routingIndexes.Length > 0 && serializer.IsEncryptionEnabled)
                {
                    if (parametersMetadata.Columns == null)
                    {
                        Logger.Warning("Could not calculate routing key for prepared statement '{0}' because columns are null. " +
                                       "Token Aware routing will not be available for this request.", PreparedStatement.Cql);
                        return;
                    }
                }

                var keys = new RoutingKey[routingIndexes.Length];
                for (var i = 0; i < routingIndexes.Length; i++)
                {
                    var index = routingIndexes[i];
                    byte[] key = serializer.SerializeAndEncrypt(PreparedStatement.Keyspace, parametersMetadata, index, valuesByPosition, index);
                    if (key == null)
                    {
                        //The partition key can not be null
                        //Get out and let any node reply a Response Error if appropriate
                        return;
                    }
                    keys[i] = new RoutingKey(key);
                }
                SetRoutingKey(keys);
                return;
            }
            if (routingNames != null && useNamedParameters)
            {
                var keys = new RoutingKey[routingNames.Length];
                var routingValues = Utils.GetValues(routingNames, rawValues[0]).ToArray();
                if (routingValues.Length != keys.Length)
                {
                    //The routing names are not valid
                    return;
                }

                if (routingValues.Length > 0 && serializer.IsEncryptionEnabled)
                {
                    if (parametersMetadata.ColumnIndexes == null)
                    {
                        Logger.Warning("Could not calculate routing key for prepared statement '{0}' because column indexes are null. " +
                                       "Token Aware routing will not be available for this request.", PreparedStatement.Cql);
                        return;
                    }
                    if (parametersMetadata.Columns == null)
                    {
                        Logger.Warning("Could not calculate routing key for prepared statement '{0}' because columns are null. " +
                                       "Token Aware routing will not be available for this request.", PreparedStatement.Cql);
                        return;
                    }
                }

                for (var i = 0; i < routingValues.Length; i++)
                {
                    byte[] key = null;
                    if (serializer.IsEncryptionEnabled)
                    {
                        if (!parametersMetadata.ColumnIndexes.TryGetValue(routingNames[i], out var colIndex))
                        {
                            Logger.Warning("Could not compute routing key because there's no column metadata for parameter {0} in PreparedStatement ({1}). " +
                                           "Token Aware routing will not be available for this request.", routingNames[i], PreparedStatement.Cql);
                        }
                        else
                        {
                            key = serializer.SerializeAndEncrypt(PreparedStatement.Keyspace, parametersMetadata, colIndex, routingValues, i);
                        }
                    }
                    else
                    {
                        key = serializer.Serialize(routingValues[i]);
                    }
                    if (key == null)
                    {
                        //The partition key can not be null
                        //Get out and let any node reply a Response Error if appropriate
                        return;
                    }
                    keys[i] = new RoutingKey(key);
                }
                SetRoutingKey(keys);
            }
        }

        public override string ToString()
        {
            return _preparedStatement?.ToString();
        }
    }
}
