using System;
using System.IO;

namespace HardLinkBackup
{
    public class BackupFileModel
    {
        public BackupFileModel(string fullFileName, string relativeFileName)
        {
            if (string.IsNullOrEmpty(fullFileName))
                throw new Exception("File path can not be empty");

            FileInfo = new FileInfoEx(fullFileName);

            RelativePathWin = PathHelpers.NormalizePathWin(relativeFileName).TrimStart('\\', '/');
            RelativePathUnix = PathHelpers.NormalizePathUnix(relativeFileName).TrimStart('\\', '/');
            RelativeDirectoryNameWin = PathHelpers.NormalizePathWin(Path.GetDirectoryName(relativeFileName)).TrimStart('\\', '/');
            RelativeDirectoryNameUnix = PathHelpers.NormalizePathUnix(Path.GetDirectoryName(relativeFileName)).TrimStart('\\', '/');
        }

        public string RelativePathWin { get; }

        public string RelativePathUnix { get; }

        public string RelativeDirectoryNameWin { get; }

        public string RelativeDirectoryNameUnix { get; }

        public FileInfoEx FileInfo { get; }
    }
}