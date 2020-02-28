namespace Backuper
{
    public class BackupParams
    {
        public string RootDit { get; set; }

        public string[] Sources { get; set; }

        public string SshLogin { get; set; }

        public string SshPassword { get; set; }

        public string SshHost { get; set; }

        public int? SshPort { get; set; }

        public string LogFile { get; set; }

        public bool IsSshDefined
        {
            get
            {
                return !string.IsNullOrEmpty(SshLogin) &&
                       !string.IsNullOrEmpty(SshHost) &&
                       !string.IsNullOrEmpty(SshPassword) &&
                       SshPort.HasValue;
            }
        }

        public string[] BackupRoots { get; set; }
        public string RemoteRootUnix { get; set; }
        public string RemoteRootWin { get; set; }
        public bool FastMode { get; set; }
    }
}