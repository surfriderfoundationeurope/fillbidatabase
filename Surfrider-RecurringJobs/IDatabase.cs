using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Surfrider.Jobs {
    public interface IDatabase
    {
        Task<ExecutedScriptStatus> ExecuteScriptAsync(string scriptPath, IDictionary<string, object> parms);
        Task<bool> ExecuteScriptsAsync(SortedList<int, string> sqlSteps, IDictionary<string, object> parms);
        Task<int> ExecuteNonQueryAsync(string query, IDictionary<string, object> args = null);
        Task<string> ExecuteStringQuery(string query, IDictionary<string, object> args = null);
    }

}