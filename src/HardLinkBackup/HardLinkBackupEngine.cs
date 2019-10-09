using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HardLinkBackup
{
    public class HardLinkBackupEngine
    {
        private const string DateFormat = "yyyy-MM-dd-HHmmss";

        private readonly string _source;
        private readonly string _destination;
        private readonly bool _allowSimultaneousReadWrite;
        private readonly IHardLinkHelper _hardLinkHelper;
        private readonly Func<string, string> _sourceFilePathConverter;
        private readonly Semaphore _fileIoSemaphore;

        public event Action<string, int> Log;

        public event Action<string> LogExt;

        public HardLinkBackupEngine(string source, string destination, bool allowSimultaneousReadWrite, IHardLinkHelper hardLinkHelper, Func<string, string> sourceFilePathConverter = null)
        {
            _source = source.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _destination = destination.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _allowSimultaneousReadWrite = allowSimultaneousReadWrite;
            _hardLinkHelper = hardLinkHelper;
            _sourceFilePathConverter = sourceFilePathConverter;
            _fileIoSemaphore = new Semaphore(1, 1);
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

            WriteLog($"Found {prevBkps.Count} backups", ++category);

            /*WriteLog("Checking integrity", ++category);

            foreach (var backupInfo in prevBkps)
            {
                backupInfo.CheckIntegrity();
            }*/

            var currentBkpDir = Path.Combine(_destination, newBkpName);
            var filesCount = files.Count;
            var currentBkp = new BackupInfo
            {
                AbsolutePath = currentBkpDir,
                DateTime = newBkpDate,
                Objects = new List<BackupFileInfo>(filesCount)
            };

            WriteLog("Fast check backups...", ++category);

            var filesExists = Directory.EnumerateFiles(_destination, "*", SearchOption.AllDirectories).ToList();

            var prevBackupFilesRaw = prevBkps
                .SelectMany(b => b.Objects.Select(fll => new {file = fll, backup = b}))
                //.Select(x => new {exists = File.Exists(x.backup.AbsolutePath + x.file.Path), finfo = x})
                .Select(x => new {exists = filesExists.Contains(x.backup.AbsolutePath + x.file.Path), finfo = x})
                .ToList();

            var deletedCount = prevBackupFilesRaw.Count(x => !x.exists);
            if (deletedCount > 0)
            {
                WriteLog($"Found {deletedCount} invalid records (file does not exist), please run health check command", ++category);
            }

            var prevBackupFiles = prevBackupFilesRaw
                .Where(x => x.exists)
                .Select(x => x.finfo)
                .ToList();

            var copiedCount = 0;
            var linkedCount = 0;
            var linkFailedCount = 0;

            WriteLog("Backing up...", ++category);

            object locker = new object();

            var processed = 0;
            var tasks = files
                .AsParallel().WithDegreeOfParallelism(2)
                .Select(async localFileInfo =>
                {
                    try
                    {
                        var processedLocal = Interlocked.Increment(ref processed);

                        var newFile = localFileInfo.FileName.Replace(_source, currentBkpDir);
                        var newFileRelativeName = newFile.Replace(currentBkpDir, string.Empty);

                        var newDir = Path.GetDirectoryName(newFile);
                        if (newDir == null)
                        {
                            throw new InvalidOperationException("Cannot get file's directory");
                        }

                        if (!Directory.Exists(newDir))
                            Directory.CreateDirectory(newDir);

                        var fileFromPrevBackup =
                            prevBackupFiles
                                .FirstOrDefault(oldFile =>
                                    oldFile.file.Length == localFileInfo.FileInfo.Length &&
                                    oldFile.file.Hash == localFileInfo.FastHashStr);

                        string existingFile;
                        if (fileFromPrevBackup != null)
                            existingFile = fileFromPrevBackup.backup.AbsolutePath + fileFromPrevBackup.file.Path;
                        else
                        {
                            lock (locker)
                            {
                                existingFile = currentBkp.Objects
                                    .FirstOrDefault(copied =>
                                        copied.Length == localFileInfo.FileInfo.Length &&
                                        copied.Hash == localFileInfo.FastHashStr)?.Path;
                            }
                            
                            if (existingFile != null)
                                existingFile = currentBkp.AbsolutePath + existingFile;
                        }

                        var needCopy = true;
                        if (existingFile != null)
                        {
                            _fileIoSemaphore.WaitOne();
                            WriteLog($"[{processedLocal} of {filesCount}] {{link}} {localFileInfo.FileName.Replace(_source, null)} ", Interlocked.Increment(ref category));
                            try
                            {
                                if (_hardLinkHelper.CreateHardLink(existingFile, newFile))
                                {
                                    needCopy = false;
                                    linkedCount++;
                                }
                                else
                                {
                                    linkFailedCount++;
                                }
                            }
                            finally
                            {
                                _fileIoSemaphore.Release();
                            }
                        }

                        if (needCopy)
                        {
                            void ProgressCallback(double progress)
                            {
                                WriteLogExt($"{progress:F2} %");
                            }

                            byte[] copiedHash;
                            try
                            {
                                _fileIoSemaphore.WaitOne();
                                WriteLog($"[{processedLocal} of {filesCount}] {localFileInfo.FileName.Replace(_source, null)} ", Interlocked.Increment(ref category));
                                copiedHash = await HashSumHelper.CopyUnbufferedAndComputeHashAsyncXX(localFileInfo.FileName, newFile, ProgressCallback, _allowSimultaneousReadWrite);
                            }
                            finally
                            {
                                _fileIoSemaphore.Release();
                            }

                            if (localFileInfo.FastHashStr == string.Concat(copiedHash.Select(b => $"{b:X}")))
                            {
                                copiedCount++;
                            }
                            else
                            {
                                Debugger.Break();
                            }

                            new FileInfo(newFile).Attributes |= FileAttributes.ReadOnly;
                        }

                        var o =  new BackupFileInfo
                        {
                            Path = newFileRelativeName,
                            Hash = localFileInfo.FastHashStr,
                            Length = localFileInfo.FileInfo.Length
                        };

                        lock (locker)
                            currentBkp.Objects.Add(o);
                        return o;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine();
                        Console.WriteLine(e);
                        Console.WriteLine();
                        return null;
                    }
                })
                .Where(x => x != null)
                .ToList();

            await Task.WhenAll(tasks);

            //var fis = tasks.Select(x => x.Result).ToList();
            //currentBkp.Objects.AddRange(fis);

            currentBkp.WriteToDisk();

            var log = "Backup done.";
            if (copiedCount > 0)
            {
                log += $" {copiedCount} files copied";
            }

            if (linkedCount > 0)
            {
                log += $" {linkedCount} files linked";
            }

            if (linkFailedCount > 0)
            {
                log += $" {linkFailedCount} link failed (files copied as duplicates)";
            }

            WriteLog(log, ++category);
        }

        private void Validate()
        {
            if (!Directory.Exists(_source))
            {
                throw new InvalidOperationException("Source directory does not exist");
            }

            if (!Directory.Exists(_destination))
            {
                throw new InvalidOperationException("Destination directory does not exist");
            }

            if (BackupInfo.DiscoverBackups(_source).Any())
            {
                throw new InvalidOperationException("Source directory contains backups. Backing up backups is not supported");
            }
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