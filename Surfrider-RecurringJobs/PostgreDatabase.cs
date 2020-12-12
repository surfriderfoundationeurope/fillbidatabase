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

        public async Task<ExecutedScriptStatus> ExecuteNonQueryScriptAsync(string scriptPath, IDictionary<string, string> parms = null)
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
                    if (parms != null)
                        command = ReplaceParamsIntoQuery(command, parms);
                    
                    command = command.Replace("\r\n", " ").Replace("\r", " ").Replace("\n", " ").Replace("\t", "    ");
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
        public async Task<ExecutedScriptStatus> ExecuteQueryScriptAsync(string scriptPath, IDictionary<string, string> parms = null)
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
                    if (parms != null)
                        command = ReplaceParamsIntoQuery(command, parms);
                    
                    command = command.Replace("\r\n", " ").Replace("\r", " ").Replace("\n", " ").Replace("\t", "    ");
                    ScriptStatus.Result = (IList<string>) await ExecuteStringQueryAsync(command, parms);
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
        public async Task<int> ExecuteNonQueryAsync(string query, IDictionary<string, string> args = null)
        {
            try
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
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                return -1;
            }
        }

        // Retrieves only the first column of the results
        public async Task<IList<string>> ExecuteStringQueryAsync(string query, IDictionary<string, string> args = null)
        {
            IList<string> res = new List<string>();
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

                            var value = reader.GetValue(0);

                            if (value == DBNull.Value)
                                return res;
                            else {
                                if (dataType == "uuid")
                                    res.Add(reader.GetGuid(0).ToString());
                                if (dataType == "boolean")
                                    res.Add(reader.GetBoolean(0).ToString());// returns "False" or "True"
                            }
                        }
                    }
                }
            }
            return res;
        }

        public async Task<bool> ExecuteScriptsAsync(SortedList<int, string> sqlSteps, IDictionary<string, string> parms)
        {
            foreach (var SqlStep in sqlSteps)
            {
                if (await ExecuteNonQueryScriptAsync(SqlStep.Value, parms).ContinueWith(x => x.Result.Status != ScriptStatusEnum.OK))
                    return false;
            }
            return true;
        }
        private string ReplaceParamsIntoQuery(string command, IDictionary<string, string> parms)
        {
            foreach (var parm in parms)
            {
                command = command.Replace(new String("@" + parm.Key), parm.Value);
            }
            return command;
        }
    }
}