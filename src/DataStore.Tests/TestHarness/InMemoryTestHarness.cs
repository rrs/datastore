namespace DataStore.Tests.TestHarness
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using CircuitBoard.MessageAggregator;
    using CircuitBoard.Messages;
    using global::DataStore.Interfaces.LowLevel;
    using global::DataStore.MessageAggregator;

    public class InMemoryTestHarness : ITestHarness
    {
        private readonly IMessageAggregator messageAggregator = DataStoreMessageAggregator.Create();

        private InMemoryTestHarness()
        {
            DocumentRepository = new InMemoryDocumentRepository();
            DataStore = new DataStore(DocumentRepository, this.messageAggregator);
        }

        public List<IMessage> AllMessages => this.messageAggregator.AllMessages.ToList();

        public DataStore DataStore { get; }

        private InMemoryDocumentRepository DocumentRepository { get; }

        public static ITestHarness Create()
        {
            return new InMemoryTestHarness();
        }

        public void AddToDatabase<T>(T aggregate) where T : class, IAggregate, new()
        {
            //copied from datastore create capabilities, may get out of date
            DataStoreCreateCapabilities.ForceProperties(aggregate.ReadOnly, aggregate);

            DocumentRepository.Aggregates.Add(aggregate);
        }

        public IEnumerable<T> QueryDatabase<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new()
        {
            var queryResult = DocumentRepository.Aggregates.OfType<T>().AsQueryable();

            queryResult = query == null ? queryResult : queryResult.Where(query);
            return queryResult;
        }
    }
}