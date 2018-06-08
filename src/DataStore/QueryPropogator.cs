using CircuitBoard.MessageAggregator;
using DataStore.Interfaces;
using DataStore.Interfaces.LowLevel;
using DataStore.Models.Messages;
using DataStore.Models.PureFunctions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace DataStore
{
    public class QueryPropogator<TQuery> : IQueryPropogator<TQuery> where TQuery : class, IAggregate, new()
    {
        private readonly IDocumentRepository dataStoreConnection;

        private readonly IMessageAggregator messageAggregator;

        private readonly Expression<Func<TQuery, bool>> query;

        public QueryPropogator(IDocumentRepository dataStoreConnection, IMessageAggregator messageAggregator, Expression<Func<TQuery, bool>> query)
        {
            this.dataStoreConnection = dataStoreConnection;
            this.messageAggregator = messageAggregator;
            this.query = query;
        }

        public Task<IEnumerable<TResult>> Select<TResult>(Expression<Func<TQuery, TResult>> select)
        {
            Guard.Against(() => query == null, "Select cannot be null when asking for a different return type to the type being queried");

            return this.messageAggregator.CollectAndForward(new TransformationQueriedOperation<TQuery, TResult>(nameof(Select), query, select))
                       .To(this.dataStoreConnection.ExecuteQuery);
        }

        public Task<IEnumerable<TQuery>> Select()
        {
            return this.messageAggregator.CollectAndForward(new ReadQueriedOperation<TQuery>(nameof(Select), query))
                       .To(this.dataStoreConnection.ExecuteQuery);
        }
    }
}
