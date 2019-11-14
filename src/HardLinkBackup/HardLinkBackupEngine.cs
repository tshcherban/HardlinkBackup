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
            ".dll",
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

            var invalidFiles = files
                .Where(x => x.FileName.Split('\\').Any(y => y.Length > 250))
                .ToList();
            if (invalidFiles.Count > 0)
            {
                WriteLog($"{invalidFiles.Count} files have paths with part longer than 250. They will not be copied", ++category);
                foreach (var f in invalidFiles)
                    files.Remove(f);
            }

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
                .SelectMany(b => b.Objects.Select(fll => new { file = fll, backup = b }))
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
            var smallFilesTarPath = $"{svcDir}/small-files.tar.gz";
            var directoriesTarPath = $"{svcDir}/dir-tree.tar.gz";

            var findHelper = new SessionFileFindHelper(currentBkp, prevBackupFiles);

            CreateDirectories(files, directoriesTarPath, ref category, s =>
            {
                var path = NormalizePathWin(s);
                path = currentBkpDir + path;
                Directory.CreateDirectory(path);
            });

            WriteLog("Backing up...", ++category);

            var smallFiles = new List<FileInfoEx>();
            for (var index = files.Count - 1; index >= 0; index--)
            {
                var file = files[index];
                if (file.FileInfo.Length < 4 * 1024 * 1024 || CompressibleFileExtensions.Contains(file.FileInfo.Extension))
                {
                    if (file.FileName.Length < 98)
                    {
                        smallFiles.Add(file);
                        files.RemoveAt(index);
                    }
                }
            }

            var processed = 0;

            ProcessSmallFiles(smallFiles, ref category, smallFilesTarPath, currentBkpDir, findHelper, filesCount, currentBkp, ref linkedCount, ref processed);

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

                    var existingFile = findHelper.FindFile(localFileInfo);

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

            WriteLog("Writing hardlinks to target", Interlocked.Increment(ref category));

            try
            {
                _hardLinkHelper.CreateHardLinks();
            }
            finally
            {
                currentBkp.WriteToDisk();
            }

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

        private void ProcessSmallFiles(List<FileInfoEx> smallFiles, ref int category, string smallFilesTarPath, string currentBkpDir, SessionFileFindHelper findHelper, int filesCount, BackupInfo currentBkp, ref int linkedCount, ref int processed)
        {
            if (smallFiles.Count > 0)
            {
                WriteLog($"Small files hash calculation...", ++category);

                var cnt = smallFiles
                    .AsParallel()
                    .Select(x =>
                    {
                        try
                        {
                            return x.FastHashStr;
                        }
                        catch (Exception exception)
                        {
                            Console.WriteLine(exception);
                            return "invalid hash: " + exception.Message;
                        }
                    })
                    .Count(x => x.StartsWith("invalid hash"));
                if (cnt > 0)
                    WriteLog($"Found {cnt} invalid records", ++category);

                WriteLog($"{smallFiles.Count} files will be transferred in a batch as tar.gz", ++category);

                bool created;
                using (var tar = new TarGzHelper(smallFilesTarPath))
                {
                    foreach (var file in smallFiles)
                    {
                        try
                        {
                            var newFile = file.FileName.Replace(_source, currentBkpDir);
                            var newFileRelativeName = newFile.Replace(currentBkpDir, string.Empty);

                            var existingFile = findHelper.FindFile(file);
                            if (existingFile != null)
                            {
                                _hardLinkHelper.AddHardLinkToQueue(existingFile, newFile);
                                linkedCount++;

                                WriteLog($"[{Interlocked.Increment(ref processed)} of {filesCount}] {{link}} {newFileRelativeName} ", Interlocked.Increment(ref category));
                            }
                            else
                            {
                                var relFileName = NormalizePathUnix(file.FileName.Replace(_source, null)).TrimStart('/');
                                using (var fl = file.FileInfo.OpenRead())
                                    tar.AddFile(relFileName, fl);

                                WriteLog($"[{Interlocked.Increment(ref processed)} of {filesCount}] {{tar}} {newFileRelativeName} ", Interlocked.Increment(ref category));
                            }

                            var o = new BackupFileInfo
                            {
                                Path = newFileRelativeName,
                                Hash = file.FastHashStr,
                                Length = file.FileInfo.Length
                            };
                            currentBkp.Objects.Add(o);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }

                    created = tar.IsArchiveCreated;
                }

                if (created)
                {
                    WriteLog("Unpacking small files", Interlocked.Increment(ref category));
                    _hardLinkHelper.UnpackTar(smallFilesTarPath);
                }
            }
        }

        private void CreateDirectories(List<FileInfoEx> files, string tarPath, ref int category, Action<string> createDir)
        {
            var dirList = files
                .Select(x => x.FileInfo.DirectoryName?.Replace(_source, null))
                .Where(x => !string.IsNullOrEmpty(x))
                .Select(x => NormalizePathUnix(x))
                .Distinct()
                .ToList();

            WriteLog("Creating directories...", ++category);

            var dirsOrder = dirList.OrderBy(x => x.Split('/').Length).ToList();

            dirList.Clear();
            while (dirsOrder.Count > 0)
            {
                var subDir = dirsOrder[dirsOrder.Count - 1];
                dirsOrder.RemoveAt(dirsOrder.Count - 1);
                dirList.Add(subDir);

                for (var i = dirsOrder.Count - 1; i >= 0; i--)
                {
                    var dir = dirsOrder[i];
                    if (subDir.StartsWith(dir + "/"))
                        dirsOrder.RemoveAt(i);
                }
            }

            bool created;
            using (var tar = new TarGzHelper(tarPath))
            {
                foreach (var dir in dirList)
                {
                    if (dir.Length > 98)
                    {
                        WriteLog($"'{dir}' too long, creating via smb...", Interlocked.Increment(ref category));
                        createDir(dir);
                    }
                    else
                    {
                        tar.AddEmptyFolder(dir);
                    }
                }

                created = tar.IsArchiveCreated;
            }

            if (created)
                _hardLinkHelper.UnpackTar(tarPath);
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