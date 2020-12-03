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

        public async Task<ExecutedScriptStatus> ExecuteScriptAsync(string scriptPath, IDictionary<string, object> parms = null)
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
                    if(parms != null)
                        command = ReplaceParamsIntoQuery(command, parms);
                    await ExecuteNonQueryAsync(command);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"-------------- ERROR DURING SQL FILE EXECUTION {scriptPath}");
                    Console.WriteLine(e.ToString());
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
                        cmd.CommandText = ReplaceParamsIntoQuery(cmd.CommandText, args);
                    }
                    return await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task<string> ExecuteStringQueryAsync(string query, IDictionary<string, object> args = null)
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
                        cmd.CommandText = ReplaceParamsIntoQuery(cmd.CommandText, args);
                    }
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (reader.Read())
                        {
                            // je suis pas fier de ça mais bon, a defaut d'avoir une meilleure solution...
                            var dataType = reader.GetDataTypeName(0);
                            if(dataType == "uuid")
                                res += reader.GetGuid(0);
                        }
                    }
                }
            }
            return res;
        }

        public async Task<bool> ExecuteScriptsAsync(SortedList<int, string> sqlSteps, IDictionary<string, object> parms)
        {
            foreach (var SqlStep in sqlSteps)
            {
                if (await ExecuteScriptAsync(SqlStep.Value, parms).ContinueWith(x => x.Result.Status != ScriptStatusEnum.OK))
                    return false;
            }
            return true;
        }
        private string ReplaceParamsIntoQuery(string command, IDictionary<string, object> parms)
        {
            foreach (var parm in parms)
            {
                command = command.Replace(new String("@" + parm.Key), (string)parm.Value);
            }
            return command;
        }
    }
}