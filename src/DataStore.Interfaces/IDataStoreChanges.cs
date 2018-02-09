using System.Collections.Generic;

namespace DataStore.Interfaces
{
    public interface IDataStoreChanges<T>
    {
        IEnumerable<T> Changed { get; }
        string ContinuationToken { get; }
    }
}
