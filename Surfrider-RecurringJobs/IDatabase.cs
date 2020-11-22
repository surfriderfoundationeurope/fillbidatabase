using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Surfrider.Jobs {
    public interface IDatabase
    {
        Task<ExecutedScriptStatus> ExecuteScript(string scriptPath, IDictionary<string, object> parms)
        Task<int> ExecuteNonQuery(string query, IDictionary<string, object> args = null);
        Task<string> ExecuteStringQuery(string query, IDictionary<string, object> args = null);
    }

}