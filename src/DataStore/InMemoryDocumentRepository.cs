namespace DataStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using global::DataStore.Interfaces;
    using global::DataStore.Interfaces.LowLevel;
    using global::DataStore.Models;
    using global::DataStore.Models.PureFunctions.Extensions;

    public class InMemoryDocumentRepository : IDocumentRepository
    {
        public InMemoryDb Db { get; set; } = new InMemoryDb();

        public Task AddAsync<T>(IDataStoreWriteOperation<T> aggregateAdded) where T : class, IAggregate, new()
        {
            Db.Add(aggregateAdded.Model);

            return Task.CompletedTask;
        }

        public IQueryable<T> CreateDocumentQuery<T>() where T : class, IAggregate, new()
        {
            //clone otherwise its to easy to change the referenced object in test code affecting results
            return Db.Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().CloneEnumerable().AsQueryable();
        }

        public Task DeleteHardAsync<T>(IDataStoreWriteOperation<T> aggregateHardDeleted) where T : class, IAggregate, new()
        {
            Db.RemoveAll(a => a.id == aggregateHardDeleted.Model.id);

            return Task.CompletedTask;
        }

        public Task DeleteSoftAsync<T>(IDataStoreWriteOperation<T> aggregateSoftDeleted) where T : class, IAggregate, new()
        {
            var aggregate = Db.Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().Single(a => a.id == aggregateSoftDeleted.Model.id);

            var now = DateTime.UtcNow;
            aggregate.Active = false;
            aggregate.Modified = now;
            aggregate.ModifiedAsMillisecondsEpochTime = now.ConvertToMillisecondsEpochTime();

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            Db.Dispose();
        }

        public Task<IEnumerable<T>> ExecuteQuery<T>(IDataStoreReadFromQueryable<T> aggregatesQueried)
        {
            //clone otherwise its to easy to change the referenced object in test code affecting results
            var result = aggregatesQueried.Query.ToList().CloneEnumerable();

            return Task.FromResult(result);
        }

        public Task<bool> Exists(IDataStoreReadById aggregateQueriedById)
        {
            return Task.FromResult(Db.Aggregates.Any(a => a.id == aggregateQueriedById.Id));
        }

        public Task<T> GetItemAsync<T>(IDataStoreReadById aggregateQueriedById) where T : class, IAggregate, new()
        {
            var aggregate = Db.Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().SingleOrDefault(a => a.id == aggregateQueriedById.Id);

            //clone otherwise its to easy to change the referenced object in test code affecting results
            return Task.FromResult(aggregate?.Clone());
        }

        public Task UpdateAsync<T>(IDataStoreWriteOperation<T> aggregateUpdated) where T : class, IAggregate, new()
        {
            var toUpdate = Db.Aggregates.Single(x => x.id == aggregateUpdated.Model.id);

            aggregateUpdated.Model.CopyProperties(toUpdate);

            return Task.CompletedTask;
        }

        public Task<IDataStoreChanges<T>> GetChangedSinceToken<T>(IDataStoreReadChanges aggregateQueriedByToken) where T : class, IAggregate, new()
        {
            var version = string.IsNullOrEmpty(aggregateQueriedByToken.Token) 
                ? 0 
                : long.Parse(aggregateQueriedByToken.Token);
            var changedRows = Db.Rows.Where(x => x.Aggregate.schema == typeof(T).FullName && x.Version > version);

            var highestVersion = changedRows.Any() ? changedRows.Max(x => x.Version).ToString() : aggregateQueriedByToken.Token;

            IDataStoreChanges<T> changes = new DataStoreChanges<T>(changedRows.Select(x => x.Aggregate).Cast<T>(), highestVersion);
            return  Task.FromResult(changes);
        }

        public class InMemoryDb : IDisposable
        {
            private readonly List<InMemoryRow> _backingStore = new List<InMemoryRow>();
            public IEnumerable<IAggregate> Aggregates => _backingStore.Select(o => o.Aggregate);
            public IEnumerable<InMemoryRow> Rows => _backingStore;

            public void Add(IAggregate aggregate)
            {
                _backingStore.Add(new InMemoryRow(aggregate));
            }

            public void RemoveAll(Predicate<IAggregate> p)
            {
                _backingStore.RemoveAll(o => p(o.Aggregate));
            }

            public void Clear() => _backingStore.Clear();

            public void Dispose()
            {
                _backingStore.Clear();
            }
        }


        public class InMemoryRow
        {
            private static long _version = 1;

            public long Version { get; } = _version++;
            public IAggregate Aggregate { get; }

            public InMemoryRow(IAggregate aggregate)
            {
                Aggregate = aggregate;
            }
        }
    }
}