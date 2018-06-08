using DataStore.Interfaces.LowLevel;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace DataStore.Interfaces
{
    public interface IQueryPropogator<TQuery> where TQuery : class, IAggregate, new()
    {
        Task<IEnumerable<TResult>> Select<TResult>(Expression<Func<TQuery, TResult>> select);

        Task<IEnumerable<TQuery>> Select();
    }
}
