namespace DataStore.Impl.SqlServer
{
    using DataStore.Interfaces;
    using DataStore.Interfaces.LowLevel;
    using DataStore.Models.PureFunctions.Extensions;
    using Newtonsoft.Json;
    using Rrs.TaskShim;
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;

    public class SqlServerRepository : IDocumentRepository
    {
        private readonly SqlServerDbClientFactory clientFactory;

        private readonly SqlServerDbSettings settings;

        public SqlServerRepository(SqlServerDbSettings settings)
        {
            this.settings = settings;
            this.clientFactory = new SqlServerDbClientFactory(settings);
            SqlServerDbInitialiser.Initialise(this.clientFactory, settings);
        }

        public Task AddAsync<T>(IDataStoreWriteOperation<T> aggregateAdded) where T : class, IAggregate, new()
        {
            using (var con = this.clientFactory.OpenClient())
            {
                using (var command = new SqlCommand(
                    $"INSERT INTO {this.settings.TableName} ([AggregateId], [Schema], [Json]) VALUES(Convert(uniqueidentifier, @AggregateId), @Schema, @Json)",
                    con))
                {
                    command.Parameters.Add(new SqlParameter("AggregateId", aggregateAdded.Model.id));

                    command.Parameters.Add(new SqlParameter("Schema", aggregateAdded.Model.schema));

                    var json = JsonConvert.SerializeObject(aggregateAdded.Model);
                    command.Parameters.Add(new SqlParameter("Json", json));

                    return Task.Factory.FromAsync(command.BeginExecuteNonQuery, command.EndExecuteNonQuery, null);
                }
            }
        }

        public IQueryable<T> CreateDocumentQuery<T>() where T : class, IAggregate, new()
        {
            var schema = typeof(T).FullName;

            var query = new List<T>();
            using (var connection = this.clientFactory.OpenClient())
            {
                using (var command = new SqlCommand($"SELECT Json FROM {this.settings.TableName} WHERE [Schema] = '{schema}'", connection))
                {
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var json = reader.GetString(0);

                            query.Add(JsonConvert.DeserializeObject<T>(json));
                        }
                    }
                }
            }
            return query.AsQueryable();
        }

        public Task DeleteHardAsync<T>(IDataStoreWriteOperation<T> aggregateHardDeleted) where T : class, IAggregate, new()
        {
            using (var con = this.clientFactory.OpenClient())
            {
                using (var command = new SqlCommand($"DELETE FROM {this.settings.TableName} WHERE AggregateId = CONVERT(uniqueidentifier, @AggregateId)", con))
                {
                    command.Parameters.Add(new SqlParameter("AggregateId", aggregateHardDeleted.Model.id));

                    return Task.Factory.FromAsync(command.BeginExecuteNonQuery, command.EndExecuteNonQuery, null);
                }
            }
        }

        public Task DeleteSoftAsync<T>(IDataStoreWriteOperation<T> aggregateSoftDeleted) where T : class, IAggregate, new()
        {
            using (var connection = this.clientFactory.OpenClient())
            {
                using (var command = new SqlCommand(
                    $"UPDATE {this.settings.TableName} SET Json = @Json WHERE AggregateId = CONVERT(uniqueidentifier, @AggregateId)",
                    connection))
                {
                    command.Parameters.Add(new SqlParameter("AggregateId", aggregateSoftDeleted.Model.id));

                    var now = DateTime.UtcNow;
                    aggregateSoftDeleted.Model.Modified = now;
                    aggregateSoftDeleted.Model.ModifiedAsMillisecondsEpochTime = now.ConvertToMillisecondsEpochTime();
                    aggregateSoftDeleted.Model.Active = false;
                    var json = JsonConvert.SerializeObject(aggregateSoftDeleted.Model);
                    command.Parameters.Add(new SqlParameter("Json", json));

                    return Task.Factory.FromAsync(command.BeginExecuteNonQuery, command.EndExecuteNonQuery, null);
                }
            }
        }

        public void Dispose()
        {
            //nothing to dispose
        }

        public Task<IEnumerable<T>> ExecuteQuery<T>(IDataStoreReadFromQueryable<T> aggregatesQueried)
        {
            var results = aggregatesQueried.Query.ToList();

            return Tap.FromResult(results.AsEnumerable());
        }

        public Task<bool> Exists(IDataStoreReadById aggregateQueriedById)
        {
            var id = aggregateQueriedById.Id;

            using (var connection = this.clientFactory.OpenClient())
            {
                using (var command = new SqlCommand(
                    $"SELECT AggregateId FROM {this.settings.TableName} WHERE AggregateId = CONVERT(uniqueidentifier, '{id}')",
                    connection))
                {
                    return Task.Factory.FromAsync(command.BeginExecuteReader, command.EndExecuteReader, null).ContinueWith(t => 
                    {
                        var result = t.Result;
                        return result != null;
                    });
                }
            }
        }

        public Task<T> GetItemAsync<T>(IDataStoreReadById aggregateQueriedById) where T : class, IAggregate, new()
        {
            // NOTE: SqlCommand.ExecuteScalarAsync() has severe performance issues when 
            //       retrieving large recordsets, therefore we use the sync implementation.

            var result = GetItem<T>(aggregateQueriedById);
            return Tap.FromResult(result);
        }

        public Task UpdateAsync<T>(IDataStoreWriteOperation<T> aggregateUpdated) where T : class, IAggregate, new()
        {
            using (var connection = this.clientFactory.OpenClient())
            {
                using (var command = new SqlCommand(
                    $"UPDATE {this.settings.TableName} SET Json = @Json WHERE AggregateId = CONVERT(uniqueidentifier, @AggregateId)",
                    connection))
                {
                    command.Parameters.Add(new SqlParameter("AggregateId", aggregateUpdated.Model.id));

                    var json = JsonConvert.SerializeObject(aggregateUpdated.Model);
                    command.Parameters.Add(new SqlParameter("Json", json));

                    return Task.Factory.FromAsync(command.BeginExecuteNonQuery, command.EndExecuteNonQuery, null);
                }
            }
        }

        private T GetItem<T>(IDataStoreReadById aggregateQueriedById) where T : class, IAggregate, new()
        {
            var id = aggregateQueriedById.Id;

            T result;
            using (var connection = this.clientFactory.OpenClient())
            {
                using (var command = new SqlCommand($"SELECT Json FROM {this.settings.TableName} WHERE AggregateId = CONVERT(uniqueidentifier, '{id}') AND [Schema] = '{typeof(T).FullName}'", connection))
                {
                    var response = command.ExecuteScalar() as string;

                    result = response == null ? null : JsonConvert.DeserializeObject<T>(response);
                }
            }
            return result;
        }
    }
}