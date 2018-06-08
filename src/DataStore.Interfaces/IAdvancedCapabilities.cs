namespace DataStore.Interfaces
{
    using DataStore.Interfaces.LowLevel;
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    public interface IAdvancedCapabilities
    {
        IQueryPropogator<T> QueryActiveCommitted<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new();

        IQueryPropogator<T> QueryCommitted<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new();

        Task<IEnumerable<T>> ReadActiveCommitted<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new();

        Task<IEnumerable<T>> ReadCommitted<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new();

        Task<T> ReadCommittedById<T>(Guid modelId) where T : class, IAggregate, new();
    }
}