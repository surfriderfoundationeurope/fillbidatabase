using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace Surfrider.Jobs
{
    public class PostgreDatabase : IDatabase
    {
        string ConnectionString;
        public PostgreDatabase(string connectionString)
        {
            this.ConnectionString = connectionString;
        }

        public async Task<ExecutedScriptStatus> ExecuteScriptAsync(string scriptPath, IDictionary<string, object> parms)
        {
            ExecutedScriptStatus ScriptStatus = new ExecutedScriptStatus();
            string command = string.Empty;
            try
            {
                command = System.IO.File.ReadAllText(scriptPath);
            }
            catch (Exception e)
            {
                Console.WriteLine($"-------------- ERROR READING SQL FILE {scriptPath}");
                ScriptStatus.Status = ScriptStatusEnum.ERROR;
                ScriptStatus.Reason = e.ToString();
            }
            if (command != string.Empty)
            {
                try
                {
                    foreach(var parm in parms){
                        command = command.Replace(new String("@" + parm.Key), (string)parm.Value);
                    }
                    await ExecuteNonQueryAsync(command);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"-------------- ERROR DURING SQL FILE EXECUTION {scriptPath}");
                    ScriptStatus.Status = ScriptStatusEnum.ERROR;
                    ScriptStatus.Reason = e.ToString();
                }
            }
            ScriptStatus.Status = ScriptStatusEnum.OK;
            return ScriptStatus;

        }
        public async Task<int> ExecuteNonQueryAsync(string query, IDictionary<string, object> args = null)
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    query = query.Replace("\r\n", " ").Replace("\r", " ").Replace("\n", " ").Replace("\t", "    ");
                    cmd.CommandText = query;
                    if (args != null)
                    {
                        foreach (var arg in args)
                        {
                            cmd.Parameters.AddWithValue(arg.Key, arg.Value);
                        }
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
                    if (args != null)
                    {
                        foreach (var arg in args)
                        {
                            cmd.Parameters.AddWithValue(arg.Key, arg.Value);
                        }
                    }
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

        public async Task<bool> ExecuteScriptsAsync(SortedList<int, string> sqlSteps, IDictionary<string, object> parms)
        {
            foreach(var SqlStep in sqlSteps){
            if (await ExecuteScriptAsync(SqlStep.Value, parms).ContinueWith(x => x.Result.Status != ScriptStatusEnum.OK))
                return false;
            }
            return true;
        }
    }
}