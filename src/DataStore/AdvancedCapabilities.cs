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


        // get a filtered list of the models from  a set of DataObjects
        public Task<T> ReadCommittedById<T>(Guid modelId) where T : class, IAggregate, new()
        {
            return this.messageAggregator.CollectAndForward(new AggregateQueriedByIdOperation(nameof(ReadCommittedById), modelId))
                       .To(this.dataStoreConnection.GetItemAsync<T>);
        }

        public IQueryPropogator<T> QueryActiveCommitted<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new()
        {
            Expression<Func<T, bool>> activeQuery = o => o.Active;

            query = query == null ? activeQuery : query.And(activeQuery);

            return new QueryPropogator<T>(dataStoreConnection, messageAggregator, query);
        }

        public IQueryPropogator<T> QueryCommitted<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new()
        {
            return new QueryPropogator<T>(dataStoreConnection, messageAggregator, query);
        }

        public Task<IEnumerable<T>> ReadActiveCommitted<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new()
        {
            Expression<Func<T, bool>> activeQuery = o => o.Active;

            query = query == null ? activeQuery : query.And(activeQuery);

            return this.messageAggregator.CollectAndForward(new ReadQueriedOperation<T>(nameof(ReadActiveCommitted), query))
                       .To(this.dataStoreConnection.ExecuteQuery);
        }

        public Task<IEnumerable<T>> ReadCommitted<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new()
        {
            return this.messageAggregator.CollectAndForward(new ReadQueriedOperation<T>(nameof(ReadCommitted), query))
                       .To(this.dataStoreConnection.ExecuteQuery);
        }
    }
}