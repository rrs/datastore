namespace DataStore.Models.Messages
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using DataStore.Interfaces;

    public class TransformationReadOperation<TQuery, TResult> : IDataStoreReadTransformOperation<TQuery, TResult>
    {
        public TransformationReadOperation(string methodCalled, Expression<Func<TQuery, TResult>> select)
        {
            MethodCalled = methodCalled;
            TypeName = typeof(TQuery).FullName;
            Select = select;
            Created = DateTime.UtcNow;
        }

        public DateTime Created { get; set; }

        public string MethodCalled { get; set; }

        public Expression<Func<TQuery, TResult>> Select { get; set; }

        public double StateOperationCost { get; set; }

        public TimeSpan? StateOperationDuration { get; set; }

        public long StateOperationStartTimestamp { get; set; }

        public long? StateOperationStopTimestamp { get; set; }

        public string TypeName { get; set; }
    }
}