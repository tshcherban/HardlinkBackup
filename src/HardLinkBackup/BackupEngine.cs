using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace HardLinkBackup
{
    public class BackupEngine
    {
        private const string DateFormat = "yyyy-MM-dd-HHmmss";

        private readonly string _source;
        private readonly string _destination;
        private readonly bool _allowSimultaneousReadWrite;

        public event Action<string, int> Log;

        public event Action<string> LogExt;

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

        private void WriteLogExt(string msg)
        {
            LogExt?.Invoke(msg);
        }

        public async Task DoBackup()
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
            var linkFailedCount = 0;

            WriteLog("Copying...", ++category);

            var processed = 0;
            foreach (var f in files)
            {
                processed++;

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

                var needCopy = true;
                if (existingFile != null)
                {
                    WriteLog($"[{processed} of {files.Count}] {{link}} {f.FileName.Replace(_source, null)} ", category);
                    if (HardLinkHelper.CreateHardLink(newFile, existingFile, IntPtr.Zero))
                    {
                        
                        needCopy = false;
                        linkedCount++;
                    }
                    else
                    {
                        linkFailedCount++;
                    }
                }
                
                if (needCopy)
                {
                    WriteLog($"[{processed} of {files.Count}] {f.FileName.Replace(_source, null)} ", category);

                    void ProgressCallback(double progress)
                    {
                        WriteLogExt($"{progress:F2} %");
                    }

                    var copiedHash = await HashSumHelper.CopyUnbufferedAndComputeHashAsync(f.FileName, newFile, ProgressCallback, _allowSimultaneousReadWrite);
                    if (f.FastHashStr == string.Concat(copiedHash.Select(b => $"{b:X}")))
                        copiedCount++;
                    else
                        Debugger.Break();
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

            WriteLog($"Backup done. {copiedCount} files copied, {linkedCount} files linked, {linkFailedCount} link failed (files copied as duplicates)", ++category);
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