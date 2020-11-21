namespace Surfrider.Jobs.Recurring
{
    public enum OperationStatus {
        OK = 0,
        WARNING = 1,
        ERROR = 2
    }
    public static class PipelineStatus {
        public static OperationStatus Status;
        public static string Reason;

        // public PipelineStatus(OperationStatus status)
        // {
        //     Status = status;
        // }

        // public PipelineStatus(OperationStatus status, string reason)
        // {
        //     this.Status = status;
        //     this.Reason = reason;
        // }
    }
}