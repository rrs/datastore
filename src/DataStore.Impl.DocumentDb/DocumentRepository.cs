﻿namespace DataStore.DataAccess.Impl.DocumentDb
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Interfaces;
    using Interfaces.Addons;
    using Messages.Events;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Models.Config;

    public class DocumentRepository : IDocumentRepository
    {
        private readonly DocumentDbSettings _config;

        private readonly DocumentClient _documentClient;

        public DocumentRepository(DocumentDbSettings config)
        {
            _documentClient = new DocumentDbClientFactory(config).GetDocumentClient();
            _config = config;
        }

        #region IDocumentRepository Members

        public async Task<T> AddAsync<T>(AggregateAdded<T> aggregateAdded) where T : IHaveAUniqueId
        {
            if (aggregateAdded == null || aggregateAdded.Model == null)
            {
                throw new ArgumentNullException(nameof(aggregateAdded));
            }

            var disableAutoIdGeneration = aggregateAdded.Model.id != Guid.Empty;

            var stopWatch = Stopwatch.StartNew();            
            var result =
                await
                    DocumentDbUtils.ExecuteWithRetries(
                        () =>
                            _documentClient.CreateDocumentAsync(
                                _config.DatabaseSelfLink(),
                                aggregateAdded.Model,
                                disableAutomaticIdGeneration: disableAutoIdGeneration));
            stopWatch.Stop();
            aggregateAdded.QueryDuration = stopWatch.Elapsed;
            aggregateAdded.QueryCost = result.RequestCharge;

            return (T) (dynamic) result.Resource;
        }

        public IQueryable<T> CreateDocumentQuery<T>() where T : IHaveAUniqueId, IHaveSchema
        {
            var name = typeof(T).Name;
            var query = _documentClient.CreateDocumentQuery<T>(_config.DatabaseSelfLink()).Where(item => item.Schema == name);
            return query;
        }

        public async Task<T> DeleteHardAsync<T>(AggregateHardDeleted<T> aggregateHardDeleted) where T : IHaveAUniqueId
        {
            var docLink = CreateDocumentSelfLinkFromId(aggregateHardDeleted.Model.id);

            var stopWatch = Stopwatch.StartNew();
            var result = await DocumentDbUtils.ExecuteWithRetries(() => _documentClient.DeleteDocumentAsync(docLink));
            stopWatch.Stop();
            aggregateHardDeleted.QueryCost = result.RequestCharge;
            aggregateHardDeleted.QueryDuration = stopWatch.Elapsed;

            return (T) (dynamic) result.Resource;
        }

        public async Task<T> DeleteSoftAsync<T>(AggregateSoftDeleted<T> aggregateSoftDeleted) where T : IHaveAUniqueId
        {
            //HACK: this call inside the doc repository is effectively duplicate [see callers] 
            //and causes us to miss this query when profiling, arguably its cheap, but still
            //if I can determine how to create an Azure Document from T we can ditch it.
            var document = await GetItemAsync(new AggregateQueriedById(nameof(DeleteSoftAsync), aggregateSoftDeleted.Model.id, typeof(T)));

            document.SetPropertyValue(nameof(IAggregate.Active), false);

            var stopWatch = Stopwatch.StartNew();
            var result = await DocumentDbUtils.ExecuteWithRetries(() => _documentClient.ReplaceDocumentAsync(document.SelfLink, document));
            stopWatch.Stop();
            aggregateSoftDeleted.QueryDuration = stopWatch.Elapsed;
            aggregateSoftDeleted.QueryCost = result.RequestCharge;

            return (T) (dynamic) result.Resource;
        }

        public void Dispose()
        {
            _documentClient.Dispose();
        }

        public async Task<IEnumerable<T>> ExecuteQuery<T>(AggregatesQueried<T> aggregatesQueried) where T : IHaveAUniqueId
        {
            var results = new List<T>();

            var documentQuery = aggregatesQueried.Query.AsDocumentQuery();
            var stopWatch = Stopwatch.StartNew();
            while (documentQuery.HasMoreResults)
            {
                var result = await DocumentDbUtils.ExecuteWithRetries(() => documentQuery.ExecuteNextAsync<T>()).ConfigureAwait(false);

                aggregatesQueried.QueryCost += result.RequestCharge;

                results.AddRange(result);
            }
            stopWatch.Stop();
            aggregatesQueried.QueryDuration = stopWatch.Elapsed;
            return results;
        }

        public async Task<T> GetItemAsync<T>(AggregateQueriedById aggregateQueriedById) where T : IHaveAUniqueId
        {
            var result = await GetItemAsync(aggregateQueriedById);
            return (T) (dynamic) result;
        }

        public async Task<Document> GetItemAsync(AggregateQueriedById aggregateQueriedById)
        {
            try
            {
                var stopWatch = Stopwatch.StartNew();
                var result = await _documentClient.ReadDocumentAsync(CreateDocumentSelfLinkFromId(aggregateQueriedById.Id));
                if (result == null)
                {
                    throw new DatabaseRecordNotFoundException(aggregateQueriedById.Id.ToString());
                }                
                stopWatch.Stop();
                aggregateQueriedById.QueryDuration = stopWatch.Elapsed;
                aggregateQueriedById.QueryCost = result.RequestCharge;

                return result.Resource;
            }
            catch (Exception e)
            {
                throw new DatabaseException($"Failed to retrieve record with id {aggregateQueriedById.Id}: {e.Message}", e);
            }
        }

        public async Task<T> UpdateAsync<T>(AggregateUpdated<T> aggregateUpdated) where T : IHaveAUniqueId
        {
            var stopWatch = Stopwatch.StartNew();
            var result =
                await
                    DocumentDbUtils.ExecuteWithRetries(
                        () => _documentClient.ReplaceDocumentAsync(CreateDocumentSelfLinkFromId(aggregateUpdated.Model.id), aggregateUpdated.Model));

            stopWatch.Stop();
            aggregateUpdated.QueryDuration = stopWatch.Elapsed;
            aggregateUpdated.QueryCost = result.RequestCharge;

            return (T) (dynamic) result.Resource;
        }

        public async Task<bool> Exists(AggregateQueriedById aggregateQueriedById)
        {
            var stopWatch = Stopwatch.StartNew();
            var query =
                _documentClient.CreateDocumentQuery(_config.DatabaseSelfLink()).Where(item => item.Id == aggregateQueriedById.Id.ToString()).AsDocumentQuery();

            var results = await query.ExecuteNextAsync();

            stopWatch.Stop();
            aggregateQueriedById.QueryDuration = stopWatch.Elapsed;
            aggregateQueriedById.QueryCost = results.RequestCharge;

            return results.Count > 0;
        }

        #endregion

        private Uri CreateDocumentSelfLinkFromId(Guid id)
        {
            if (Guid.Empty == id)
            {
                throw new ArgumentException("Id is required for update/delete/read operation");
            }

            var docLink = UriFactory.CreateDocumentUri(_config.DatabaseName, _config.DefaultCollectionName, id.ToString());
            return docLink;
        }
    }
}