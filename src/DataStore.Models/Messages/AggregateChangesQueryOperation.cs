namespace DataStore.Models.Messages
{
    using System;
    using DataStore.Interfaces;

    public class AggregateChangesQueryOperation : IDataStoreReadChanges
    {
        public AggregateChangesQueryOperation(string methodCalled, string token, Type type = null)
        {
            MethodCalled = methodCalled;
            Token = token;
            TypeName = type?.FullName;
            Created = DateTime.UtcNow;
        }

        public DateTime Created { get; set; }

        public string Token { get; set; }

        public string MethodCalled { get; set; }

        public double StateOperationCost { get; set; }

        public TimeSpan? StateOperationDuration { get; set; }

        public long StateOperationStartTimestamp { get; set; }

        public long? StateOperationStopTimestamp { get; set; }

        public string TypeName { get; set; }
    }
}