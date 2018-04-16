namespace DataStore.Impl.SqlServer
{
    using DataStore.Interfaces;
    using DataStore.Interfaces.LowLevel;
    using DataStore.Models;
    using DataStore.Models.PureFunctions.Extensions;
    using Newtonsoft.Json;
    using Rrs.TaskShim;
    using System;
    using System.Collections;
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
            var json = JsonConvert.SerializeObject(aggregateAdded.Model);

            var p = new(string, object)[]
            {
                ("AggregateId", aggregateAdded.Model.id),
                ("Schema", aggregateAdded.Model.schema),
                ("Json", json)
            };

            var command = $"INSERT INTO {this.settings.TableName} ([AggregateId], [Schema], [Json]) VALUES(Convert(uniqueidentifier, @AggregateId), @Schema, @Json)";

            return ExecuteNonQueryAsync(command, p);
        }

        public IQueryable<T> CreateDocumentQuery<T>() where T : class, IAggregate, new()
        {
            var schema = typeof(T).FullName;

            var query = new List<T>();
            using (var connection = this.clientFactory.OpenClient())
            using (var command = new SqlCommand($"SELECT Json FROM {this.settings.TableName} WHERE [Schema] = '{schema}'", connection))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var json = reader.GetString(0);

                    query.Add(JsonConvert.DeserializeObject<T>(json));
                }
            }
            return query.AsQueryable();
        }

        public Task DeleteHardAsync<T>(IDataStoreWriteOperation<T> aggregateHardDeleted) where T : class, IAggregate, new()
        {
            var p = new(string, object)[]
            {
                ("AggregateId", aggregateHardDeleted.Model.id),
            };

            var command = $"DELETE FROM {this.settings.TableName} WHERE AggregateId = CONVERT(uniqueidentifier, @AggregateId)";

            return ExecuteNonQueryAsync(command, p);
        }

        public Task DeleteSoftAsync<T>(IDataStoreWriteOperation<T> aggregateSoftDeleted) where T : class, IAggregate, new()
        {
            var now = DateTime.UtcNow;
            aggregateSoftDeleted.Model.Modified = now;
            aggregateSoftDeleted.Model.ModifiedAsMillisecondsEpochTime = now.ConvertToMillisecondsEpochTime();
            aggregateSoftDeleted.Model.Active = false;
            var json = JsonConvert.SerializeObject(aggregateSoftDeleted.Model);

            var p = new(string, object)[]
            {
                ("AggregateId", aggregateSoftDeleted.Model.id),
                ("Json", json)
            };
            var command = $"UPDATE {this.settings.TableName} SET Json = @Json WHERE AggregateId = CONVERT(uniqueidentifier, @AggregateId)";

            return ExecuteNonQueryAsync(command, p);
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


            var command = $"SELECT AggregateId FROM {this.settings.TableName} WHERE AggregateId = CONVERT(uniqueidentifier, '{id}')";

            return ExecuteScalarAsync<Guid?>(command).ContinueWith(t => t.Result != null);
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
            var json = JsonConvert.SerializeObject(aggregateUpdated.Model);

            var p = new(string, object)[]
            {
                ("AggregateId", aggregateUpdated.Model.id),
                ("Json", json)
            };

            var command = $"UPDATE {this.settings.TableName} SET Json = @Json WHERE AggregateId = CONVERT(uniqueidentifier, @AggregateId)";

            return ExecuteNonQueryAsync(command, p);
            
        }

        public Task<IDataStoreChanges<T>> GetChangedSinceToken<T>(IDataStoreReadChanges aggregateQueriedByToken) where T : class, IAggregate, new()
        {
            var schema = typeof(T).FullName;

            var rowVersion = string.IsNullOrEmpty(aggregateQueriedByToken.Token) 
                ? new byte[8] 
                : Convert.FromBase64String(aggregateQueriedByToken.Token);

            var p = new(string, object)[]
            {
                ("RowVersion", rowVersion),
            };

            var command = $"SELECT Json, Version FROM {this.settings.TableName} WHERE [Schema] = '{schema}' AND Version > @RowVersion";

            byte[] highestRowVersion = rowVersion;

            IEnumerable<T> readerFunc(SqlDataReader reader)
            {
                var query = new List<T>();

                while (reader.Read())
                {
                    var json = reader.GetString(0);
                    var rowVer = (byte[])reader.GetValue(1);
                    if (((IStructuralComparable)rowVer).CompareTo(highestRowVersion, Comparer<byte>.Default) > 0) highestRowVersion = rowVer;
                    query.Add(JsonConvert.DeserializeObject<T>(json));
                }

                return query;
            }

            return ExecuteQueryAsync(command, readerFunc, p).ContinueWith(t =>
            {
                var items = t.Result;
                IDataStoreChanges<T> changes = new DataStoreChanges<T>(items, Convert.ToBase64String(highestRowVersion));
                return changes;
            });
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

                    result = response.FromJsonString<T>();
                }
            }
            return result;
        }

        private Task ExecuteNonQueryAsync(string commandText, IEnumerable<(string, object)> parameters = null)
        {
            var con = this.clientFactory.OpenClient();
            var command = new SqlCommand(commandText, con);

            foreach(var (name, value) in parameters ?? Enumerable.Empty<(string, object)>())
            {
                command.Parameters.Add(new SqlParameter(name, value));
            }

            return Task.Factory.FromAsync(command.BeginExecuteNonQuery, r =>
            {
                using (con)
                using (command)
                {
                    command.EndExecuteNonQuery(r);
                }
            }, null);
        }

        private Task<T> ExecuteScalarAsync<T>(string commandText, IEnumerable<(string, object)> parameters = null)
        {
            var con = this.clientFactory.OpenClient();
            var command = new SqlCommand(commandText, con);

            foreach (var (name, value) in parameters ?? Enumerable.Empty<(string, object)>())
            {
                command.Parameters.Add(new SqlParameter(name, value));
            }

            return Task.Factory.FromAsync(command.BeginExecuteReader, r =>
            {
                using (con)
                using (command)
                using (var reader = command.EndExecuteReader(r))
                {
                    return reader.Read() ? (T)reader[0] : default(T);
                }
            }, null);
        }

        private Task<IEnumerable<T>> ExecuteQueryAsync<T>(string commandText, Func<SqlDataReader, IEnumerable<T>> readerFunc,  IEnumerable<(string, object)> parameters = null)
        {
            var con = this.clientFactory.OpenClient();
            var command = new SqlCommand(commandText, con);

            foreach (var (name, value) in parameters ?? Enumerable.Empty<(string, object)>())
            {
                command.Parameters.Add(new SqlParameter(name, value));
            }

            return Task.Factory.FromAsync(command.BeginExecuteReader, r =>
            {
                using (con)
                using (command)
                using (var reader = command.EndExecuteReader(r))
                {
                    return readerFunc(reader);
                }
            }, null);
        }
    }
}