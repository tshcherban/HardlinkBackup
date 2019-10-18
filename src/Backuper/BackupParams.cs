namespace Backuper
{
    public class BackupParams
    {
        public string Source { get; set; }

        public string Target { get; set; }

        public string SshLogin { get; set; }

        public string SshPassword { get; set; }

        public string SshRootDir { get; set; }

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
                       !string.IsNullOrEmpty(SshRootDir) &&
                       SshPort.HasValue;
            }
        }
    }
}