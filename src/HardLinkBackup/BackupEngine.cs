using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;

namespace HardLinkBackup
{
    public class BackupEngine
    {
        private const string DateFormat = "yyyy-MM-dd-HHmmss";

        private readonly string _source;
        private readonly string _destination;
        private readonly bool _allowSimultaneousReadWrite;

        public event Action<string, int> Log;

        public BackupEngine(string source, string destination, bool allowSimultaneousReadWrite)
        {
            _source = source.TrimEnd('\\');
            _destination = destination.TrimEnd('\\');
            _allowSimultaneousReadWrite = allowSimultaneousReadWrite;
        }

        private void WriteLog(string msg, int category)
        {
            Log?.Invoke(msg, category);
        }

        public void DoBackup()
        {
            Validate();

            var category = 0;
            WriteLog("Enumerating files...", ++category);

            var files = Directory.EnumerateFiles(_source, "*", SearchOption.AllDirectories)
                .Select(f => new FileInfoEx(f))
                .ToList();

            WriteLog("Discovering backups...", ++category);

            var newBkpDate = DateTime.Now;
            var newBkpName = newBkpDate.ToString(DateFormat, CultureInfo.InvariantCulture);
            var prevBkps = BackupInfo.DiscoverBackups(_destination).ToList();

            /*WriteLog("Checking integrity", ++category);

            foreach (var backupInfo in prevBkps)
            {
                backupInfo.CheckIntegrity();
            }*/

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

            WriteLog("Copying...", ++category);

            var processed = 1;
            foreach (var f in files)
            {
                WriteLog($"{f.FileName} ({processed++} of {files.Count})", category);

                var newFile = f.FileName.Replace(_source, currentBkpDir);
                var newFileRelativeName = newFile.Replace(currentBkpDir, string.Empty);

                var newDir = Path.GetDirectoryName(newFile);
                if (newDir == null)
                    throw new InvalidOperationException("Cannot get file's directory");

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
                }
                else
                {
                    var copiedHash = HashSumHelper.CopyUnbufferedAndComputeHashAsync(f.FileName, newFile, p => { }, _allowSimultaneousReadWrite).Result;
                    if (f.FastHashStr == string.Concat(copiedHash.Select(b => $"{b:X}")))
                    copiedCount++;
                }

                var fi = new FileInfoEx(newFile);
                fi.FileInfo.Attributes = fi.FileInfo.Attributes | FileAttributes.ReadOnly;
                currentBkp.Objects.Add(new BackupFileInfo
                {
                    Path = newFileRelativeName,
                    Hash = fi.FastHashStr,
                    Length = fi.FileInfo.Length
                });
            }

            currentBkp.WriteToDisk();

            WriteLog($"Backup done. {copiedCount} files copied, {linkedCount} files linked", ++category);
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

        private static void CreatePar(string file, BackupInfo currentBkp)
        {
            var parFile = currentBkp.AbsolutePath + $"\\.bkp\\par{file.Replace(currentBkp.AbsolutePath, null)}.par";
            var proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "par2j64",
                    Arguments = $"c /rr10 /rf1 \"{parFile}\" \"{file}\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                }
            };
            proc.Start();
            proc.WaitForExit();
            var outp = proc.StandardOutput.ReadToEnd();
        }
    }
}