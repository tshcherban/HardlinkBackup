using System;

namespace HardLinkBackup
{
    public class BackupFileInfo
    {
        public string Path { get; set; }

        public string Hash { get; set; }

        public long Length { get; set; }

        public DateTime Created { get; set; }

        public DateTime Modified { get; set; }

        /// <summary>
        /// Runtime only property, always false if read from existing backup
        /// </summary>
        public bool IsLink { get; set; }
    }
}