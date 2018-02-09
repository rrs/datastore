namespace DataStore.Impl.DocumentDb
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using DataStore.Impl.DocumentDb.Config;
    using DataStore.Interfaces;
    using DataStore.Interfaces.LowLevel;
    using DataStore.Models;
    using DataStore.Models.Messages;
    using DataStore.Models.PureFunctions.Extensions;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;

    public class DocumentDbRepository : IDocumentRepository
    {
        private readonly DocumentDbSettings config;

        private readonly DocumentClient documentClient;

        public DocumentDbRepository(DocumentDbSettings config)
        {
            this.documentClient = new DocumentDbClientFactory(config).GetDocumentClient();
            this.config = config;
        }

        public async Task AddAsync<T>(IDataStoreWriteOperation<T> aggregateAdded) where T : class, IAggregate, new()
        {
            if (aggregateAdded == null || aggregateAdded.Model == null)
            {
                throw new ArgumentNullException(nameof(aggregateAdded));
            }

            var result = await DocumentDbUtils
                             .ExecuteWithRetries(() => this.documentClient.CreateDocumentAsync(this.config.CollectionSelfLink(), aggregateAdded.Model))
                             .ConfigureAwait(false);

            aggregateAdded.StateOperationCost = result.RequestCharge;
        }

        public IQueryable<T> CreateDocumentQuery<T>() where T : class, IAggregate, new()
        {
            var name = typeof(T).FullName;
            var query = this.documentClient.CreateDocumentQuery<T>(
                this.config.CollectionSelfLink(),
                new FeedOptions
                {
                    EnableCrossPartitionQuery = this.config.CollectionSettings.EnableCrossParitionQueries,
                    MaxDegreeOfParallelism = -1,
                    MaxBufferedItemCount = -1
                }).Where(item => item.schema == name);
            return query;
        }

        public async Task DeleteHardAsync<T>(IDataStoreWriteOperation<T> aggregateHardDeleted) where T : class, IAggregate, new()
        {
            var docLink = CreateDocumentSelfLinkFromId(aggregateHardDeleted.Model.id);

            var result = await DocumentDbUtils.ExecuteWithRetries(() => this.documentClient.DeleteDocumentAsync(docLink)).ConfigureAwait(false);
            aggregateHardDeleted.StateOperationCost = result.RequestCharge;
        }

        public async Task DeleteSoftAsync<T>(IDataStoreWriteOperation<T> aggregateSoftDeleted) where T : class, IAggregate, new()
        {
            //HACK: this call inside the doc repository is effectively duplicate [see callers] 
            //and causes us to miss this query when profiling, arguably its cheap, but still
            //if I can determine how to create an Azure Document from T we can ditch it.
            var document = await GetItemAsync(new AggregateQueriedByIdOperation(nameof(DeleteSoftAsync), aggregateSoftDeleted.Model.id, typeof(T)))
                               .ConfigureAwait(false);

            var now = DateTime.UtcNow;
            document.SetPropertyValue(nameof(IAggregate.Active), false);
            document.SetPropertyValue(nameof(IAggregate.Modified), now);
            document.SetPropertyValue(nameof(IAggregate.ModifiedAsMillisecondsEpochTime), now.ConvertToMillisecondsEpochTime());

            var result = await DocumentDbUtils.ExecuteWithRetries(() => this.documentClient.ReplaceDocumentAsync(document.SelfLink, document)).ConfigureAwait(false);

            aggregateSoftDeleted.StateOperationCost = result.RequestCharge;
        }

        public void Dispose()
        {
            this.documentClient.Dispose();
        }

        public async Task<IEnumerable<T>> ExecuteQuery<T>(IDataStoreReadFromQueryable<T> aggregatesQueried)
        {
            var results = new List<T>();

            var documentQuery = aggregatesQueried.Query.AsDocumentQuery();

            while (documentQuery.HasMoreResults)
            {
                var result = await DocumentDbUtils.ExecuteWithRetries(() => documentQuery.ExecuteNextAsync<T>()).ConfigureAwait(false);

                aggregatesQueried.StateOperationCost += result.RequestCharge;

                results.AddRange(result);
            }

            return results;
        }

        public async Task<bool> Exists(IDataStoreReadById aggregateQueriedById)
        {
            var query = this.documentClient.CreateDocumentQuery(this.config.CollectionSelfLink()).Where(item => item.Id == aggregateQueriedById.Id.ToString())
                            .AsDocumentQuery();

            var results = await query.ExecuteNextAsync().ConfigureAwait(false);

            aggregateQueriedById.StateOperationCost = results.RequestCharge;

            return results.Count > 0;
        }

        public async Task<T> GetItemAsync<T>(IDataStoreReadById aggregateQueriedById) where T : class, IAggregate, new()
        {
            var result = await GetItemAsync(aggregateQueriedById).ConfigureAwait(false);
            return (T)result;
        }

        public async Task<dynamic> GetItemAsync(IDataStoreReadById aggregateQueriedById)
        {
            try
            {
                if (aggregateQueriedById.Id == Guid.Empty) return null; //createdocumentselflink will fail otherwise
                var result = await this.documentClient.ReadDocumentAsync(CreateDocumentSelfLinkFromId(aggregateQueriedById.Id)).ConfigureAwait(false);
                if (result == null)
                {
                    throw new DatabaseRecordNotFoundException(aggregateQueriedById.Id.ToString());
                }

                aggregateQueriedById.StateOperationCost = result.RequestCharge;

                return result.Resource;
            }
            catch (DocumentClientException de)
            {
                if (de.StatusCode == HttpStatusCode.NotFound) //handle when it doesn't exists and return null
                {
                    return null;
                }
                throw new DatabaseException($"Failed to retrieve record with id {aggregateQueriedById.Id}: {de.Message}", de);
            }
            catch (Exception e)
            {
                throw new DatabaseException($"Failed to retrieve record with id {aggregateQueriedById.Id}: {e.Message}", e);
            }
        }

        public async Task UpdateAsync<T>(IDataStoreWriteOperation<T> aggregateUpdated) where T : class, IAggregate, new()
        {
            var result = await DocumentDbUtils.ExecuteWithRetries(
                             () => this.documentClient.ReplaceDocumentAsync(
                                 CreateDocumentSelfLinkFromId(aggregateUpdated.Model.id),
                                 aggregateUpdated.Model)).ConfigureAwait(false);

            aggregateUpdated.StateOperationCost = result.RequestCharge;
        }

        private Uri CreateDocumentSelfLinkFromId(Guid id)
        {
            if (Guid.Empty == id)
            {
                throw new ArgumentException("id is required for update/delete/read operation");
            }

            var docLink = UriFactory.CreateDocumentUri(this.config.DatabaseName, this.config.CollectionSettings.CollectionName, id.ToString());
            return docLink;
        }

        public async Task<IDataStoreChanges<T>> GetChangedSinceToken<T>(IDataStoreReadChanges aggregateQueriedByToken) where T : class, IAggregate, new()
        {
            throw new NotImplementedException();
            // TODO not tested in anyway, just filled in what i know so far
            var checkpoints = JsonConvert.DeserializeObject<Dictionary<string, string>>(aggregateQueriedByToken.Token);

            var (docs, newCheckpoints) = await GetChanges(documentClient, UriFactory.CreateDocumentCollectionUri(config.DatabaseName, config.CollectionSettings.CollectionName), checkpoints);

            return new DataStoreChanges<T>(docs.Cast<T>(), JsonConvert.SerializeObject(newCheckpoints));
        }

        /// <summary>
        /// Get changes within the collection since the last checkpoint. This sample shows how to process the change 
        /// feed from a single worker. When working with large collections, this is typically split across multiple
        /// workers each processing a single or set of partition key ranges.
        /// </summary>
        /// <param name="client">DocumentDB client instance</param>
        /// <param name="checkpoints"></param>
        /// <returns></returns>
        private static async Task<(IEnumerable<Document>, Dictionary<string, string>)> GetChanges(
            DocumentClient client,
            Uri collectionUri,
            Dictionary<string, string> checkpoints)
        {
            //int numChangesRead = 0;
            string pkRangesResponseContinuation = null;
            List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();

            do
            {
                FeedResponse<PartitionKeyRange> pkRangesResponse = await client.ReadPartitionKeyRangeFeedAsync(
                    collectionUri,
                    new FeedOptions { RequestContinuation = pkRangesResponseContinuation });

                partitionKeyRanges.AddRange(pkRangesResponse);
                pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
            }
            while (pkRangesResponseContinuation != null);

            // TODO populate
            var docs = new List<Document>();

            foreach (PartitionKeyRange pkRange in partitionKeyRanges)
            {
                string continuation = null;
                checkpoints.TryGetValue(pkRange.Id, out continuation);

                IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                    collectionUri,
                    new ChangeFeedOptions
                    {
                        PartitionKeyRangeId = pkRange.Id,
                        StartFromBeginning = true,
                        RequestContinuation = continuation,
                        MaxItemCount = -1,
                    });

                while (query.HasMoreResults)
                {
                    // could change DeviceReading type to dynamic to see what it is, in another example from ms its actually
                    // a Document
                    FeedResponse<DeviceReading> readChangesResponse = await query.ExecuteNextAsync<DeviceReading>();

                    foreach (DeviceReading changedDocument in readChangesResponse)
                    {
                        // hopefully can get the doc here
                        //docs.Add()
                        //Console.WriteLine("\tRead document {0} from the change feed.", changedDocument.);
                        //numChangesRead++;
                    }

                    checkpoints[pkRange.Id] = readChangesResponse.ResponseContinuation;
                }
            }

            //Console.WriteLine("Read {0} documents from the change feed", numChangesRead);

            return (docs, checkpoints);
        }

        public class DeviceReading
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("deviceId")]
            public string DeviceId { get; set; }

            [JsonConverter(typeof(IsoDateTimeConverter))]
            [JsonProperty("readingTime")]
            public DateTime ReadingTime { get; set; }

            [JsonProperty("metricType")]
            public string MetricType { get; set; }

            [JsonProperty("unit")]
            public string Unit { get; set; }

            [JsonProperty("metricValue")]
            public double MetricValue { get; set; }
        }
    }
}