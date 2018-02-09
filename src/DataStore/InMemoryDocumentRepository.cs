﻿namespace DataStore
{
    using global::DataStore.Interfaces;
    using global::DataStore.Interfaces.LowLevel;
    using global::DataStore.Models.PureFunctions.Extensions;
    using Rrs.TaskShim;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class InMemoryDocumentRepository : IDocumentRepository
    {
        public List<IAggregate> Aggregates { get; set; } = new List<IAggregate>();

        public Task AddAsync<T>(IDataStoreWriteOperation<T> aggregateAdded) where T : class, IAggregate, new()
        {
            Aggregates.Add(aggregateAdded.Model);

            return Tap.CompletedTask;
        }

        public IQueryable<T> CreateDocumentQuery<T>() where T : class, IAggregate, new()
        {
            //clone otherwise its to easy to change the referenced object in test code affecting results
            return Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().CloneEnumerable().AsQueryable();
        }

        public Task DeleteHardAsync<T>(IDataStoreWriteOperation<T> aggregateHardDeleted) where T : class, IAggregate, new()
        {
            Aggregates.RemoveAll(a => a.id == aggregateHardDeleted.Model.id);

            return Tap.CompletedTask;
        }

        public Task DeleteSoftAsync<T>(IDataStoreWriteOperation<T> aggregateSoftDeleted) where T : class, IAggregate, new()
        {
            var aggregate = Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().Single(a => a.id == aggregateSoftDeleted.Model.id);

            var now = DateTime.UtcNow;
            aggregate.Active = false;
            aggregate.Modified = now;
            aggregate.ModifiedAsMillisecondsEpochTime = now.ConvertToMillisecondsEpochTime();

            return Tap.CompletedTask;
        }

        public void Dispose()
        {
            Aggregates.Clear();
        }

        public Task<IEnumerable<T>> ExecuteQuery<T>(IDataStoreReadFromQueryable<T> aggregatesQueried)
        {
            //clone otherwise its to easy to change the referenced object in test code affecting results
            var result = aggregatesQueried.Query.ToList().CloneEnumerable();

            return Tap.FromResult(result);
        }

        public Task<bool> Exists(IDataStoreReadById aggregateQueriedById)
        {
            var result = Aggregates.Exists(a => a.id == aggregateQueriedById.Id);

            return Tap.FromResult(result);
        }

        public Task<T> GetItemAsync<T>(IDataStoreReadById aggregateQueriedById) where T : class, IAggregate, new()
        {
            var aggregate = Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().SingleOrDefault(a => a.id == aggregateQueriedById.Id);

            //clone otherwise its to easy to change the referenced object in test code affecting results
            var clone = aggregate?.Clone();

            return Tap.FromResult(clone);
        }

        public Task UpdateAsync<T>(IDataStoreWriteOperation<T> aggregateUpdated) where T : class, IAggregate, new()
        {
            var toUpdate = Aggregates.Single(x => x.id == aggregateUpdated.Model.id);

            aggregateUpdated.Model.CopyProperties(toUpdate);

            return Tap.CompletedTask;
        }
    }
}