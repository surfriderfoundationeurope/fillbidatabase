using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace Surfrider
{
    public class PostgreDatabase : IDatabase
    {
        string ConnectionString;
        public PostgreDatabase(string connectionString)
        {
            this.ConnectionString = connectionString;
        }

        public async Task<int> ExecuteNonQuery(string query, IDictionary<string, object> args = null)
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    foreach(var arg in args){
                        cmd.Parameters.AddWithValue(arg.Key, arg.Value);
                    }
                    return await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task<string> ExecuteStringQuery(string query, IDictionary<string, object> args = null)
        {
            string res = string.Empty;
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (reader.Read())
                        {
                            res += reader.GetString(0);
                        }
                    }
                }
            }
            return res;
        }
    }
}