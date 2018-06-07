namespace DataStore
{
    using CircuitBoard.MessageAggregator;
    using global::DataStore.Interfaces;
    using global::DataStore.Interfaces.LowLevel;
    using global::DataStore.Models.Messages;
    using global::DataStore.Models.PureFunctions;
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    public class AdvancedCapabilities : IAdvancedCapabilities
    {
        private readonly IDocumentRepository dataStoreConnection;

        private readonly IMessageAggregator messageAggregator;

        public AdvancedCapabilities(IDocumentRepository dataStoreConnection, IMessageAggregator messageAggregator)
        {
            this.dataStoreConnection = dataStoreConnection;
            this.messageAggregator = messageAggregator;
        }

        // get a filtered list of the models from a set of active DataObjects
        public Task<IEnumerable<TResult>> ReadActiveCommitted<TQuery, TResult>(Expression<Func<TQuery, bool>> query, Expression<Func<TQuery, TResult>> select) where TQuery : class, IAggregate, new()
        {
            Guard.Against(() => query == null, "Queryable cannot be null when asking for a different return type to the type being queried");

            query = query.And(a => a.Active);

            return ReadCommittedInternal(query, select);
        }

        public Task<IEnumerable<TResult>> ReadCommitted<TQuery, TResult>(Expression<Func<TQuery, bool>> query, Expression<Func<TQuery, TResult>> select) where TQuery : class, IAggregate, new()
        {
            Guard.Against(() => query == null, "Queryable cannot be null when asking for a different return type to the type being queried");

            return ReadCommittedInternal(query, select);
        }

        private Task<IEnumerable<TResult>> ReadCommittedInternal<TQuery, TResult>(Expression<Func<TQuery, bool>> query, Expression<Func<TQuery, TResult>> select) where TQuery : class, IAggregate, new()
        {
            return this.messageAggregator.CollectAndForward(new TransformationQueriedOperation<TQuery, TResult>(nameof(ReadCommittedInternal), query, select))
                       .To(this.dataStoreConnection.ExecuteQuery);
        }

        // get a filtered list of the models from  a set of DataObjects
        public Task<T> ReadCommittedById<T>(Guid modelId) where T : class, IAggregate, new()
        {
            return this.messageAggregator.CollectAndForward(new AggregateQueriedByIdOperation(nameof(ReadCommittedById), modelId))
                       .To(this.dataStoreConnection.GetItemAsync<T>);
        }


        public Task<IEnumerable<T>> ReadActiveCommitted<T>(Expression<Func<T, bool>> query) where T : class, IAggregate, new()
        {
            Guard.Against(() => query == null, "Queryable cannot be null when asking for a different return type to the type being queried");

            query = query.And(a => a.Active);

            return ReadCommittedInternal(query);
        }

        public Task<IEnumerable<T>> ReadCommitted<T>(Expression<Func<T, bool>> query) where T : class, IAggregate, new()
        {
            Guard.Against(() => query == null, "Queryable cannot be null when asking for a different return type to the type being queried");

            return ReadCommittedInternal(query);
        }

        private Task<IEnumerable<T>> ReadCommittedInternal<T>(Expression<Func<T, bool>> query) where T : class, IAggregate, new()
        {
            return this.messageAggregator.CollectAndForward(new ReadQueriedOperation<T>(nameof(ReadCommittedInternal), query))
                       .To(this.dataStoreConnection.ExecuteQuery);
        }

    }
}