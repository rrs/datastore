namespace DataStore
{
    using CircuitBoard.MessageAggregator;
    using global::DataStore.Interfaces;
    using global::DataStore.Interfaces.LowLevel;
    using global::DataStore.Models.Messages;
    using Rrs.TaskShim;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    //methods return the latest version of an object including uncommitted session changes

    public class DataStoreQueryCapabilities : IDataStoreQueryCapabilities
    {
        private readonly EventReplay eventReplay;

        private readonly IMessageAggregator messageAggregator;

        public DataStoreQueryCapabilities(IDocumentRepository dataStoreConnection, IMessageAggregator messageAggregator)
        {
            this.messageAggregator = messageAggregator;
            this.eventReplay = new EventReplay(messageAggregator);
            DbConnection = dataStoreConnection;
        }

        private IDocumentRepository DbConnection { get; }

        public Task<bool> Exists(Guid id)
        {
            if (id == Guid.Empty) return Tap.FromResult(false);

            if (HasBeenHardDeletedInThisSession(id)) return Tap.FromResult(false);

            return this.messageAggregator.CollectAndForward(new AggregateQueriedByIdOperation(nameof(Exists), id)).To(DbConnection.Exists);
        }

        // get a filtered list of the models from set of DataObjects
        public Task<IEnumerable<T>> Read<T>(Expression<Func<T, bool>> predicate = null) where T : class, IAggregate, new()
        {
            var queryable = DbConnection.CreateDocumentQuery<T>();

            if (predicate != null) queryable = queryable.Where(predicate);

            var results = this.messageAggregator.CollectAndForward(new AggregatesQueriedOperation<T>(nameof(ReadActiveById), queryable))
                                    .To(DbConnection.ExecuteQuery)
                                    .ContinueWith(t => eventReplay.ApplyAggregateEvents(t.Result, false).AsEnumerable());

            return results;
        }

        // get a filtered list of the models from a set of active DataObjects
        public Task<IEnumerable<T>> ReadActive<T>(Expression<Func<T, bool>> predicate = null) where T : class, IAggregate, new()
        {
            var queryable = DbConnection.CreateDocumentQuery<T>().Where(a => a.Active);

            if (predicate != null) queryable = queryable.Where(predicate);

            var results = this.messageAggregator.CollectAndForward(new AggregatesQueriedOperation<T>(nameof(ReadActiveById), queryable))
                                    .To(DbConnection.ExecuteQuery)
                                    .ContinueWith(t => eventReplay.ApplyAggregateEvents(t.Result, true).AsEnumerable());

            return results;
        }

        // get a filtered list of the models from  a set of DataObjects
        public Task<T> ReadActiveById<T>(Guid modelId) where T : class, IAggregate, new()
        {
            if (modelId == Guid.Empty) return null;

            return messageAggregator
                .CollectAndForward(new AggregateQueriedByIdOperation(nameof(ReadActiveById), modelId))
                .To(DbConnection.GetItemAsync<T>)
                .ContinueWith(t => 
                {
                    if (t.Result == null || !t.Result.Active)
                    {
                        var replayResult = this.eventReplay.ApplyAggregateEvents(new List<T>(), true).SingleOrDefault();
                        return replayResult;
                    }

                    return this.eventReplay.ApplyAggregateEvents(
                    new List<T>
                    {
                        t.Result
                    },
                    true).SingleOrDefault();
                });
        }

        private bool HasBeenHardDeletedInThisSession(Guid id)
        {
            //if its been deleted in this session (this takes the place of eventReplay for this function)
            if (this.messageAggregator.AllMessages.OfType<IQueuedDataStoreWriteOperation>().ToList()
                    .Exists(e => e.AggregateId == id && e.GetType() == typeof(QueuedHardDeleteOperation<>)))
            {
                return true;
            }
            return false;
        }
    }
}