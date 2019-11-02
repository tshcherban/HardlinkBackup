using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HardLinkBackup
{
    internal class SessionFileHelper
    {
        private readonly BackupInfo _currentBkp;
        private readonly List<Tuple<BackupFileInfo, BackupInfo>> _prevBackupFiles;

        public SessionFileHelper(BackupInfo currentBkp, List<Tuple<BackupFileInfo, BackupInfo>> prevBackupFiles)
        {
            this._currentBkp = currentBkp;
            _prevBackupFiles = prevBackupFiles;
        }


        public string FindFile(FileInfoEx fileInfo)
        {
            var fileFromPrevBackup =
                _prevBackupFiles
                    .FirstOrDefault(oldFile =>
                        oldFile.Item1.Length == fileInfo.FileInfo.Length &&
                        oldFile.Item1.Hash == fileInfo.FastHashStr);

            string existingFile;
            if (fileFromPrevBackup != null)
                existingFile = fileFromPrevBackup.Item2.AbsolutePath + fileFromPrevBackup.Item1.Path;
            else
            {
                existingFile = _currentBkp.Objects
                    .FirstOrDefault(copied =>
                        copied.Length == fileInfo.FileInfo.Length &&
                        copied.Hash == fileInfo.FastHashStr)?.Path;

                if (existingFile != null)
                    existingFile = _currentBkp.AbsolutePath + existingFile;
            }

            return existingFile;
        }
    }
}
