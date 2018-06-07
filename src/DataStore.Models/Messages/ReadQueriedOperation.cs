﻿namespace DataStore.Models.Messages
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using DataStore.Interfaces;

    public class ReadQueriedOperation<T> : IDataStoreReadFromQueryable<T>
    {
        public ReadQueriedOperation(string methodCalled, Expression<Func<T, bool>> query)
        {
            MethodCalled = methodCalled;
            TypeName = typeof(T).FullName;
            Query = query;
            Created = DateTime.UtcNow;
        }

        public DateTime Created { get; set; }

        public string MethodCalled { get; set; }

        public Expression<Func<T, bool>> Query { get; set; }

        public double StateOperationCost { get; set; }

        public TimeSpan? StateOperationDuration { get; set; }

        public long StateOperationStartTimestamp { get; set; }

        public long? StateOperationStopTimestamp { get; set; }

        public string TypeName { get; set; }
    }
}