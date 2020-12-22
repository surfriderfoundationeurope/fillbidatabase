namespace Surfrider.Jobs
{
    public enum ScriptStatusEnum {
        OK = 0,
        WARNING = 1,
        ERROR = 2
    }
    public class ExecutedScriptStatus {
        public ScriptStatusEnum Status;
        public string Reason;
        public object Result;

    }
}