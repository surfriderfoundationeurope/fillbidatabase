using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Surfrider.Jobs {
    public interface IDatabase
    {
        Task<ExecutedScriptStatus> ExecuteNonQueryScriptAsync(string scriptPath, IDictionary<string, string> parms = null);
        Task<bool> ExecuteScriptsAsync(SortedList<int, string> sqlSteps, IDictionary<string, string> parms = null);
        Task<int> ExecuteNonQueryAsync(string query, IDictionary<string, string> args = null);
        Task<IList<string>> ExecuteStringQueryAsync(string query, IDictionary<string, string> args = null);
    }

}