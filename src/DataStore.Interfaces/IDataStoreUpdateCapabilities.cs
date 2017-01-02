namespace DataStore.Interfaces
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    public interface IDataStoreUpdateCapabilities
    {
        Task<T> UpdateById<T>(Guid id, Action<T> action, bool overwriteReadOnly = false) where T : IAggregate;

        Task<T> Update<T>(T src, bool overwriteReadOnly = false) where T : IAggregate;

        Task<IEnumerable<T>> UpdateWhere<T>(
            Expression<Func<T, bool>> predicate, 
            Action<T> action, 
            bool overwriteReadOnly = false) where T : IAggregate;
    }
}