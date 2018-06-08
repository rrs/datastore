using DataStore.Interfaces;
using DataStore.Interfaces.LowLevel;
using DataStore.Models.PureFunctions.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DataStore
{
    public class InMemoryDocumentRepository : IDocumentRepository
    {
        public List<IAggregate> Aggregates { get; set; } = new List<IAggregate>();

        public Task AddAsync<T>(IDataStoreWriteOperation<T> aggregateAdded) where T : class, IAggregate, new()
        {
            Aggregates.Add(aggregateAdded.Model);

            return Task.CompletedTask;
        }


        public Task DeleteHardAsync<T>(IDataStoreWriteOperation<T> aggregateHardDeleted) where T : class, IAggregate, new()
        {
            Aggregates.RemoveAll(a => a.id == aggregateHardDeleted.Model.id);

            return Task.CompletedTask;
        }

        public Task DeleteSoftAsync<T>(IDataStoreWriteOperation<T> aggregateSoftDeleted) where T : class, IAggregate, new()
        {
            var aggregate = Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().Single(a => a.id == aggregateSoftDeleted.Model.id);

            var now = DateTime.UtcNow;
            aggregate.Active = false;
            aggregate.Modified = now;
            aggregate.ModifiedAsMillisecondsEpochTime = now.ConvertToMillisecondsEpochTime();

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            Aggregates.Clear();
        }

        public Task<IEnumerable<T>> ExecuteQuery<T>(IDataStoreReadFromQueryable<T> aggregatesQueried) where T : class, IAggregate, new()
        {
            //clone otherwise its to easy to change the referenced object in test code affecting results
            var aggregates = Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().Clone();

            var queryable = aggregates.AsQueryable();

            queryable = aggregatesQueried.Query == null ? queryable : queryable.Where(aggregatesQueried.Query);

            var result = queryable.AsEnumerable();

            return Task.FromResult(result);
        }

        public Task<IEnumerable<TResult>> ExecuteQuery<TQuery, TResult>(IDataStoreReadTransformFromQueryable<TQuery, TResult> aggregatesQueried) where TQuery : class, IAggregate, new()
        {
            //clone otherwise its to easy to change the referenced object in test code affecting results
            var aggregates = Aggregates.Where(x => x.schema == typeof(TQuery).FullName).Cast<TQuery>().Clone();

            var queryable = aggregates.AsQueryable();

            queryable = aggregatesQueried.Query == null ? queryable : queryable.Where(aggregatesQueried.Query);

            var result = queryable.Select(aggregatesQueried.Select).AsEnumerable();

            return Task.FromResult(result);
        }

        public Task<bool> Exists(IDataStoreReadById aggregateQueriedById)
        {
            return Task.FromResult(Aggregates.Exists(a => a.id == aggregateQueriedById.Id));
        }

        public Task<T> GetItemAsync<T>(IDataStoreReadById aggregateQueriedById) where T : class, IAggregate, new()
        {
            var aggregate = Aggregates.Where(x => x.schema == typeof(T).FullName).Cast<T>().SingleOrDefault(a => a.id == aggregateQueriedById.Id);

            //clone otherwise its to easy to change the referenced object in test code affecting results
            return Task.FromResult(aggregate?.Clone());
        }

        public Task UpdateAsync<T>(IDataStoreWriteOperation<T> aggregateUpdated) where T : class, IAggregate, new()
        {
            var toUpdate = Aggregates.Single(x => x.id == aggregateUpdated.Model.id);

            aggregateUpdated.Model.CopyProperties(toUpdate);

            return Task.CompletedTask;
        }

    }
}