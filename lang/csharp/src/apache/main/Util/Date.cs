using System;

namespace Avro.Util
{
    /// <summary>
    /// The 'date' logical type.
    /// </summary>
    public class Date : LogicalType
    {
        /// <summary>
        /// The logical type name for Date.
        /// </summary>
        public static readonly string LogicalTypeName = "date";

        private static readonly DateTime _unixEpoc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Date;

        /// <summary>
        /// Initializes a new Date logical type.
        /// </summary>
        public Date() : base(LogicalTypeName)
        { }

        /// <summary>
        /// Applies 'date' logical type validation for a given logical schema.
        /// </summary>
        /// <param name="schema">The schema to be validated.</param>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Int != schema.BaseSchema.Tag)
                throw new AvroTypeException("Date can only be used with an underlying int type");
        }

        /// <summary>
        /// Converts a logical Date to an integer representing the number of days since the Unix EPOC.
        /// </summary>
        /// <param name="logicalValue">The logical date to convert.</param>
        public override object ConvertToBaseValue(object logicalValue)
        {
            var date = ((DateTime)logicalValue).Date;
            return (date - _unixEpoc).Days;
        }

        /// <summary>
        /// Convers an integer representing the number of days since the Unix EPOC to a logical Date.
        /// </summary>
        /// <param name="baseValue">The number of days since the Unix EPOC.</param>
        public override object ConvertToLogicalValue(object baseValue)
        {
            var noDays = (int)baseValue;
            return _unixEpoc.AddDays(noDays);
        }

        /// <summary>
        /// Retrieve the .NET type that is represented by the logical type implementation.
        /// </summary>
        /// <param name="nullible">A flag indicating whether it should be nullible.</param>
        public override string GetCSharpTypeName(bool nullible)
        {
            return nullible ? "System.Nullable<DateTime>" : typeof(DateTime).ToString();
        }
    }
}
