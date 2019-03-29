/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;

namespace Avro.Util
{
    /// <summary>
    /// The 'time-micros' logical type.
    /// </summary>
    public class TimeMicrosecond : LogicalUnixEpochType<TimeSpan>
    {
        /// <summary>
        /// The logical type name for TimeMicrosecond.
        /// </summary>
        public static readonly string LogicalTypeName = "time-micros";

        /// <summary>
        /// Initializes a new TimeMicrosecond logical type.
        /// </summary>
        public TimeMicrosecond() : base(LogicalTypeName)
        { }

        /// <summary>
        /// Applies 'time-micros' logical type validation for a given logical schema.
        /// </summary>
        /// <param name="schema">The schema to be validated.</param>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Long != schema.BaseSchema.Tag)
                throw new AvroTypeException("'time-micros' can only be used with an underlying long type");
        }

        /// <summary>
        /// Converts a logical TimeMicrosecond to a long representing the number of microseconds after midnight.
        /// </summary>
        /// <param name="logicalValue">The logical time to convert.</param>
        public override object ConvertToBaseValue(object logicalValue)
        {
            var time = (TimeSpan)logicalValue;
            return (long)(time - UnixEpocDateTime.TimeOfDay).TotalMilliseconds * 1000;
        }

        /// <summary>
        /// Convers a long representing the number of microseconds after midnight to a logical TimeMicrosecond.
        /// </summary>
        /// <param name="baseValue">The number of microseconds after midnight.</param>
        public override object ConvertToLogicalValue(object baseValue)
        {
            var noMs = (long)baseValue / 1000;
            return UnixEpocDateTime.TimeOfDay.Add(TimeSpan.FromMilliseconds(noMs));
        }
    }
}
