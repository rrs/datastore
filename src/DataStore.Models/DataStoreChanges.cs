using DataStore.Interfaces;
using System.Collections.Generic;

namespace DataStore.Models
{
    public class DataStoreChanges<T> : IDataStoreChanges<T>
    {
        public IEnumerable<T> Changed { get; }

        public string ContinuationToken { get; }

        public DataStoreChanges(IEnumerable<T> changes, string continuationToken)
        {
            Changed = changes;
            ContinuationToken = continuationToken;
        }
    }
}
