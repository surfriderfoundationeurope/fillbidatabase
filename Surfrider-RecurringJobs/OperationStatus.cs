namespace Surfrider.Jobs
{
    public enum OperationStatus {
        OK = 0,
        WARNING = 1,
        ERROR = 2
    }
    public class PipelineStatus {
        public OperationStatus Status;
        public string Reason;
    }
}