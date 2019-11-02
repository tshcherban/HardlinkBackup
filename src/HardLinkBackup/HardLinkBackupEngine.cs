using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharpCompress.Common;
using SharpCompress.Compressors;
using SharpCompress.Compressors.Deflate;
using SharpCompress.Writers.Tar;

namespace HardLinkBackup
{
    public class HardLinkBackupEngine
    {
        private const string DateFormat = "yyyy-MM-dd-HHmmss";

        private readonly string _source;
        private readonly string _destination;
        private readonly bool _allowSimultaneousReadWrite;
        private readonly IHardLinkHelper _hardLinkHelper;
        private readonly Func<string[]> _fileEnumerator;

        public event Action<string, int> Log;

        public event Action<string> LogExt;

        public HardLinkBackupEngine(string source, string destination, bool allowSimultaneousReadWrite, IHardLinkHelper hardLinkHelper, Func<string[]> fileEnumerator)
        {
            _source = source.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _destination = destination.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _allowSimultaneousReadWrite = allowSimultaneousReadWrite;
            _hardLinkHelper = hardLinkHelper;
            _fileEnumerator = fileEnumerator;
        }

        private void WriteLog(string msg, int category)
        {
            Log?.Invoke(msg, category);
        }

        private void WriteLogExt(string msg)
        {
            LogExt?.Invoke(msg);
        }

        private static string NormalizePathWin(string path, char separator = '\\')
        {
            return path?.Replace('/', separator).Replace('\\', separator);
        }

        private static string NormalizePathUnix(string path, char separator = '/')
        {
            return path?.Replace('\\', separator).Replace('/', separator);
        }

        private static readonly HashSet<string> CompressibleFileExtensions = new HashSet<string>
        {
            ".xml",
            ".txt",
            ".pdb",
        };

        public async Task DoBackup()
        {
            await Task.Yield();

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

            string[] filesExists;
            try
            {
                filesExists = _fileEnumerator().Select(x => NormalizePathWin(x)).ToArray();
            }
            catch
            {
                Log?.Invoke("Failed to enumerate files fast, going slow route...", ++category);

                filesExists = Directory.EnumerateFiles(_destination, "*", SearchOption.AllDirectories).ToArray();
            }

            var filesExists1 = new HashSet<string>(filesExists, StringComparer.OrdinalIgnoreCase);

            var prevBackupFilesRaw = prevBkps
                .SelectMany(b => b.Objects.Select(fll => new {file = fll, backup = b}))
                .Select(x =>
                {
                    var contains = filesExists1.Contains(NormalizePathWin(x.backup.AbsolutePath + x.file.Path));
                    return new
                    {
                        exists = contains,
                        finfo = x
                    };
                })
                .ToList();

            var deletedCount = prevBackupFilesRaw.Count(x => !x.exists);
            if (deletedCount > 0)
            {
                var ress = prevBackupFilesRaw.Where(x => !x.exists).ToList();
                WriteLog($"Found {deletedCount} invalid records (file does not exist), please run health check command", ++category);
            }

            var prevBackupFiles = prevBackupFilesRaw
                .Where(x => x.exists)
                .Select(x => Tuple.Create(x.finfo.file, x.finfo.backup))
                .ToList();

            var copiedCount = 0;
            var linkedCount = 0;

            var svcDir = currentBkp.CreateFolders();
            var tarFilePath = $"{svcDir}/files.tar.gz";

            var hlp = new SessionFileHelper(currentBkp, prevBackupFiles);

            var dirList = files
                .Select(x => x.FileInfo.DirectoryName?.Replace(_source, currentBkpDir))
                .Where(x => !string.IsNullOrEmpty(x))
                .Distinct()
                .ToList();

            WriteLog("Creating directories...", ++category);
            foreach (var dir in dirList)
            {
                if (!Directory.Exists(dir))
                    Directory.CreateDirectory(dir);
            }

            WriteLog("Backing up...", ++category);

            var smallFiles = new List<FileInfoEx>();
            for (var index = files.Count - 1; index >= 0; index--)
            {
                var file = files[index];
                if (file.FileInfo.Length < 4 * 1024 * 1024 || CompressibleFileExtensions.Contains(file.FileInfo.Extension))
                {
                    smallFiles.Add(file);
                    files.RemoveAt(index);
                }
            }

            if (smallFiles.Count > 0)
            {
                WriteLog($"{smallFiles.Count} files will be transferred in a batch as tar.gz", ++category);

                using (var outStream = File.Create(tarFilePath))
                using (var gzStream = new GZipStream(outStream, CompressionMode.Compress, CompressionLevel.BestSpeed))
                using (var tarArchive = new TarWriter(gzStream, new TarWriterOptions(CompressionType.None, true)
                {
                    ArchiveEncoding = new ArchiveEncoding()
                    {
                        Default = Encoding.UTF8,
                        Forced = Encoding.UTF8,
                    },
                }))
                {
                    for (var index = smallFiles.Count - 1; index >= 0; index--)
                    {
                        var file = smallFiles[index];
                        var newFile = file.FileName.Replace(_source, currentBkpDir);
                        var newFileRelativeName = newFile.Replace(currentBkpDir, string.Empty);

                        var existingFile = hlp.FindFile(file);
                        if (existingFile != null)
                        {
                            _hardLinkHelper.AddHardLinkToQueue(existingFile, newFile);
                            smallFiles.RemoveAt(index);
                            linkedCount++;
                        }
                        else
                        {
                            var relFileName = NormalizePathUnix(file.FileName.Replace(_source, null)).TrimStart('/');
                            using (var fl = file.FileInfo.OpenRead())
                                tarArchive.Write(relFileName, fl, file.FileInfo.LastAccessTime);
                        }

                        var o = new BackupFileInfo
                        {
                            Path = newFileRelativeName,
                            Hash = file.FastHashStr,
                            Length = file.FileInfo.Length
                        };
                        currentBkp.Objects.Add(o);
                    }
                }

                if (smallFiles.Count == 0)
                    File.Delete(tarFilePath);
            }


            var processed = 0;
            foreach (var localFileInfo in files)
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

                    var existingFile = hlp.FindFile(localFileInfo);

                    if (existingFile != null)
                    {
                        WriteLog($"[{processedLocal} of {filesCount}] {{link}} {localFileInfo.FileName.Replace(_source, null)} ", Interlocked.Increment(ref category));
                        _hardLinkHelper.AddHardLinkToQueue(existingFile, newFile);
                        linkedCount++;
                    }
                    else
                    {
                        void ProgressCallback(double progress)
                        {
                            WriteLogExt($"{progress:F2} %");
                        }

                        WriteLog($"[{processedLocal} of {filesCount}] {localFileInfo.FileName.Replace(_source, null)} ", Interlocked.Increment(ref category));
                        var copiedHash = HashSumHelper.CopyUnbufferedAndComputeHashAsyncXX(localFileInfo.FileName, newFile, ProgressCallback, _allowSimultaneousReadWrite).Result;

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

                    var o = new BackupFileInfo
                    {
                        Path = newFileRelativeName,
                        Hash = localFileInfo.FastHashStr,
                        Length = localFileInfo.FileInfo.Length
                    };

                    currentBkp.Objects.Add(o);
                }
                catch (Exception e)
                {
                    Console.WriteLine();
                    Console.WriteLine(e);
                    Console.WriteLine();
                }
            }

            if (smallFiles.Count > 0)
            {
                WriteLog("Unpacking small files", Interlocked.Increment(ref category));
                _hardLinkHelper.UnpackTar(tarFilePath);
            }

            WriteLog("Writing hardlinks to target", Interlocked.Increment(ref category));
            _hardLinkHelper.CreateHardLinks();

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