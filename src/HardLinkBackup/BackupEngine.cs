using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace HardLinkBackup
{
    public class BackupEngine
    {
        private readonly string _source;
        private readonly string _destination;
        private const string DateFormat = "yyyy-MM-dd-HHmmss";

        public BackupEngine(string source, string destination)
        {
            _source = source;
            _destination = destination;
        }

        public void DoBackup()
        {
            Validate();

            var files = Directory.EnumerateFiles(_source, "*", SearchOption.AllDirectories)
                .Select(f => new FileInfoEx(f))
                .ToList();

            var newBkpDate = DateTime.Now;
            var newBkpName = newBkpDate.ToString(DateFormat, CultureInfo.InvariantCulture);
            var prevBkps = BackupInfo.DiscoverBackups(_destination).ToList();
            foreach (var backupInfo in prevBkps)
            {
                backupInfo.CheckIntegrity();
            }

            var currentBkpDir = Path.Combine(_destination, newBkpName);
            var currentBkp = new BackupInfo
            {
                AbsolutePath = currentBkpDir,
                DateTime = newBkpDate,
                Objects = new List<BackupFileInfo>(files.Count)
            };

            var prevBackupFiles = prevBkps
                .SelectMany(b => b.Objects.Select(fll => new { file = fll, backup = b }))
                .ToList();
            var copiedCount = 0;
            var linkedCount = 0;

            foreach (var f in files)
            {
                var newFile = f.FileName.Replace(_source, currentBkpDir);
                var newFileRelativeName = newFile.Replace(currentBkpDir, string.Empty);

                var newDir = Path.GetDirectoryName(newFile);
                if (!Directory.Exists(newDir))
                    Directory.CreateDirectory(newDir);

                var fileFromPrevBackup =
                    prevBackupFiles
                        .FirstOrDefault(oldFile =>
                            oldFile.file.Length == f.FileInfo.Length &&
                            oldFile.file.Hash == f.FastHashStr);

                string existingFile;
                if (fileFromPrevBackup != null)
                    existingFile = fileFromPrevBackup.backup.AbsolutePath + fileFromPrevBackup.file.Path;
                else
                {
                    existingFile = currentBkp.Objects
                        .FirstOrDefault(copied =>
                            copied.Length == f.FileInfo.Length &&
                            copied.Hash == f.FastHashStr)?.Path;
                    if (existingFile != null)
                        existingFile = currentBkp.AbsolutePath + existingFile;
                }

                if (existingFile != null)
                {
                    if (!HardLinkHelper.CreateHardLink(newFile, existingFile, IntPtr.Zero))
                        throw new InvalidOperationException("Hardlink failed");

                    linkedCount++;
                    //Console.WriteLine($"Hardlinked\r\n{newFile}\r\nto\r\n{existingFile}");
                }
                else
                {
                    File.Copy(f.FileName, newFile);
                    copiedCount++;
                    

                    //Console.WriteLine($"Copied\r\n{newFile}\r\nfrom\r\n{f.FileName}");
                }

                var fi = new FileInfoEx(newFile);
                fi.FileInfo.Attributes |= FileAttributes.ReadOnly;
                currentBkp.Objects.Add(new BackupFileInfo
                {
                    Path = newFileRelativeName,
                    Hash = fi.FastHashStr,
                    Length = fi.FileInfo.Length
                });
            }

            currentBkp.WriteToDisk();
            Console.WriteLine($"Backup done. {copiedCount} files copied, {linkedCount} files linked");
            /*if (tasks.Count > 0)
            {
                Console.WriteLine("Waiting par...");
                Task.WaitAll(tasks.ToArray());
                Console.WriteLine("Par completed");
            }*/
        }

        private void Validate()
        {
            if (!Directory.Exists(_source))
                throw new InvalidOperationException("Source directory does not exist");

            if (!Directory.Exists(_destination))
                throw new InvalidOperationException("Destination directory does not exist");

            if (BackupInfo.DiscoverBackups(_source).Any())
                throw new InvalidOperationException("Source directory contains backups. Backing up backups is not supported");

            
        }
    }
}