using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Surfrider.Jobs {
    public interface IDatabase
    {
        Task<ExecutedScriptStatus> ExecuteScriptAsync(string scriptPath, IDictionary<string, string> parms);
        Task<bool> ExecuteScriptsAsync(SortedList<int, string> sqlSteps, IDictionary<string, string> parms);
        Task<int> ExecuteNonQueryAsync(string query, IDictionary<string, string> args = null);
        Task<string> ExecuteStringQueryAsync(string query, IDictionary<string, string> args = null);
    }

}