namespace DataStore.Interfaces
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using DataStore.Interfaces.LowLevel;

    public interface IAdvancedCapabilities
    {
        Task<IEnumerable<T>> ReadActiveCommitted<T>(Expression<Func<T, bool>> query) where T : class, IAggregate, new();

        Task<IEnumerable<T>> ReadCommitted<T>(Expression<Func<T, bool>> query) where T : class, IAggregate, new();

        Task<IEnumerable<TResult>> ReadActiveCommitted<TQuery, TResult>(Expression<Func<TQuery, bool>> query, Expression<Func<TQuery, TResult>> select) where TQuery : class, IAggregate, new();

        Task<IEnumerable<TResult>> ReadCommitted<TQuery, TResult>(Expression<Func<TQuery, bool>> query, Expression<Func<TQuery, TResult>> select) where TQuery : class, IAggregate, new();

        Task<T> ReadCommittedById<T>(Guid modelId) where T : class, IAggregate, new();
    }
}