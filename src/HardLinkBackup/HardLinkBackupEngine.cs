using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HardLinkBackup
{
    public class HardLinkBackupEngine
    {
        private const string BackupFolderNamingDateFormat = "yyyy-MM-dd-HHmmss";

        private static readonly HashSet<string> CompressibleFileExtensions = new HashSet<string>
        {
            ".xml",
            ".txt",
            ".pdb",
            ".dll",
            ".log",
        };

        //private readonly string _rootDir;
        private readonly string _destination;
        private readonly FilesSource[] _sources;
        private readonly string[] _backupRoots;
        private readonly bool _allowSimultaneousReadWrite;
        private readonly IHardLinkHelper _hardLinkHelper;
        private readonly Func<string, string[]> _remoteFilesEnumerator;
        private bool _fastMode;
        private int _category;

        public event Action<string, int> Log;

        public event Action<string> LogExt;

        public HardLinkBackupEngine(string rootDir, string[] sources, string[] backupRoots, string destination, bool allowSimultaneousReadWrite, IHardLinkHelper hardLinkHelper, Func<string, string[]> remoteFilesEnumerator, bool fastMode)
        {
            //_rootDir = rootDir.EndsWith(@":\\") ? rootDir : rootDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _destination = destination.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _sources = GetSources(rootDir, sources);
            _backupRoots = backupRoots;
            _allowSimultaneousReadWrite = allowSimultaneousReadWrite;
            _hardLinkHelper = hardLinkHelper;
            _remoteFilesEnumerator = remoteFilesEnumerator;
            _fastMode = fastMode;
        }

        private static FilesSource[] GetSources(string rootDir, string[] sources)
        {
            var ret = new FilesSource[sources.Length];
            var usedNames = new HashSet<string>();

            if (sources.Length == 1)
            {
                var source = sources[0];
                ret[0] = new FilesSource(source, null);
                return ret;
            }

            for (var index = 0; index < sources.Length; index++)
            {
                var source = sources[index];
                var relative = source.Split('\\').Last();
                if (relative.EndsWith(":"))
                {
                    relative = null;
                }
                else
                {
                    if (usedNames.Contains(relative))
                        relative += "_1";

                    usedNames.Add(relative);
                }

                var sm = new FilesSource(source, relative);
                ret[index] = sm;
            }

            return ret;
        }

        private void WriteLog(string msg, int category)
        {
            Log?.Invoke(msg, category);
        }

        private void WriteLogExt(string msg)
        {
            LogExt?.Invoke(msg);
        }

        private List<BackupFileModel> GetFilesToBackup(ref int category)
        {
            WriteLog("Enumerating files...", ++_category);

            var files = new List<BackupFileModel>();

            foreach (var src in _sources)
            {
                var fil = PathHelpers.GetDirectoryFiles(src.FullPath, "*", SearchOption.AllDirectories)
                    .Select(fullFileName =>
                    {
                        try
                        {
                            var relativeFileName = fullFileName.Replace(src.FullPath, src.Alias);

                            return new BackupFileModel(fullFileName, relativeFileName);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"Failed to access file {fullFileName}: {e.Message} ({e.GetType().Name})");
                            return null;
                        }
                    })
                    .Where(x => x != null)
                    .ToList();

                files.AddRange(fil);
            }

            var invalidFiles = files
                .Where(x => x.RelativePathWin.Split('\\').Any(y => y.Length > 250))
                .ToList();

            if (invalidFiles.Count > 0)
            {
                WriteLog($"{invalidFiles.Count} files have paths with part longer than 250. They will not be copied", ++_category);
                foreach (var f in invalidFiles)
                {
                    WriteLog(f.RelativePathWin, ++_category);
                    files.Remove(f);
                }
            }

            return files;
        }

        private void DetectChangesFast(IReadOnlyList<BackupFileModel> localFiles, List<Tuple<BackupFileInfo, BackupInfo>> prevBackupFiles, out List<BackupFileModel> changed, out List<BackupFileModel> unchanged)
        {
            changed = new List<BackupFileModel>(localFiles.Count);
            unchanged = new List<BackupFileModel>(localFiles.Count);

            var byPathLookup = prevBackupFiles.ToDictionary(x => x.Item1.Path.TrimStart('\\'));

            foreach (var localFile in localFiles)
            {
                if (byPathLookup.TryGetValue(localFile.RelativePathWin, out var backupFile))
                {
                    if (backupFile.Item1.Length == localFile.FileInfo.FileInfo.Length &&
                        Math.Abs((backupFile.Item1.Created - localFile.FileInfo.FileInfo.CreationTime).TotalSeconds) < 1 &&
                        Math.Abs((backupFile.Item1.Modified - localFile.FileInfo.FileInfo.LastWriteTime).TotalSeconds) < 1)
                    {
                        unchanged.Add(localFile);
                        continue;
                    }
                }

                changed.Add(localFile);
            }
        }

        private void DoFastBackup(BackupInfo lastFastBackup, BackupInfo currentBkp, List<BackupFileModel> localFiles)
        {
            var prevBackupFiles = GetFilesFromPrevBackups(new[] {lastFastBackup});

            var copiedCount = 0;
            var linkedCount = 0;
            var filesCount = localFiles.Count;

            var findHelper = new SessionFileFindHelper(currentBkp, prevBackupFiles);

            var processed = 0;

            DetectChangesFast(localFiles, prevBackupFiles, out var changed, out var unchanged);
            if (changed.Count == 0)
            {
                WriteLog("No changes found", ++_category);
                return;
            }

            WriteLog("Backing up...", ++_category);

            var svcDir = currentBkp.CreateFolders();
            currentBkp.CreateIncompleteAttribute();

            var smallFilesTarPath = Path.Combine(svcDir, "small-files.tar.gz");
            var directoriesTarPath = Path.Combine(svcDir, "dir-tree.tar.gz");

            CreateDirectories(localFiles, directoriesTarPath, ref _category);

            var smallFiles = GetFilesForCompression(changed);
            CopySmallFiles(smallFiles, smallFilesTarPath, currentBkp.AbsolutePath, findHelper, filesCount, currentBkp, ref linkedCount, ref processed);

            foreach (var localFileInfo in changed)
            {
                try
                {
                    var processedLocal = Interlocked.Increment(ref processed);

                    var newFile = Path.Combine(currentBkp.AbsolutePath, localFileInfo.RelativePathWin);
                    var newFileRelativeName = newFile.Replace(currentBkp.AbsolutePath, string.Empty);

                    var newDir = Path.GetDirectoryName(newFile);
                    if (newDir == null)
                    {
                        throw new InvalidOperationException("Cannot get file's directory");
                    }

                    void ProgressCallback(double progress)
                    {
                        WriteLogExt($"{progress:F2} %");
                    }

                    WriteLog($"[{processedLocal} of {filesCount}] {localFileInfo.RelativePathWin} ", Interlocked.Increment(ref _category));
                    var copiedHash = HashSumHelper.CopyUnbufferedAndComputeHashAsyncXX(localFileInfo.FileInfo.FileName, newFile, ProgressCallback, _allowSimultaneousReadWrite).Result;

                    if (localFileInfo.FileInfo.FastHashStr == string.Concat(copiedHash.Select(b => $"{b:X}")))
                    {
                        copiedCount++;
                    }
                    else
                    {
                        WriteLog($"{localFileInfo.RelativePathWin} copy failed", Interlocked.Increment(ref _category));
                        System.Diagnostics.Debugger.Break();
                    }

                    new FileInfo(newFile).Attributes |= FileAttributes.ReadOnly;

                    var o = new BackupFileInfo
                    {
                        Path = newFileRelativeName,
                        Hash = localFileInfo.FileInfo.FastHashStr,
                        Length = localFileInfo.FileInfo.FileInfo.Length,
                        IsLink = false,
                        Modified = localFileInfo.FileInfo.FileInfo.LastWriteTime,
                        Created = localFileInfo.FileInfo.FileInfo.CreationTime,
                    };

                    currentBkp.AddFile(o);
                }
                catch (Exception e)
                {
                    Console.WriteLine();
                    Console.WriteLine(e);
                    Console.WriteLine();
                }
            }

            foreach (var localFileInfo in unchanged)
            {
                try
                {
                    var processedLocal = Interlocked.Increment(ref processed);

                    var newFile = Path.Combine(currentBkp.AbsolutePath, localFileInfo.RelativePathWin);
                    var newFileRelativeName = newFile.Replace(currentBkp.AbsolutePath, string.Empty);

                    var newDir = Path.GetDirectoryName(newFile);
                    if (newDir == null)
                    {
                        throw new InvalidOperationException("Cannot get file's directory");
                    }

                    if (!Directory.Exists(newDir))
                    {
                        System.Diagnostics.Debugger.Break();
                        throw null;
                    }

                    var existingFile = findHelper.FindByLengthAndRelativePath(localFileInfo);
                    WriteLog($"[{processedLocal} of {filesCount}] {{link}} {localFileInfo.RelativePathWin} ", Interlocked.Increment(ref _category));
                    _hardLinkHelper.AddHardLinkToQueue(existingFile.path, newFile);
                    linkedCount++;

                    var o = new BackupFileInfo
                    {
                        Path = newFileRelativeName,
                        Hash = existingFile.hash,
                        Length = localFileInfo.FileInfo.FileInfo.Length,
                        IsLink = true,
                        Modified = localFileInfo.FileInfo.FileInfo.LastWriteTime,
                        Created = localFileInfo.FileInfo.FileInfo.CreationTime,
                    };

                    currentBkp.AddFile(o);
                }
                catch (Exception e)
                {
                    Console.WriteLine();
                    Console.WriteLine(e);
                    Console.WriteLine();
                }
            }

            WriteLog("Writing hardlinks to target", Interlocked.Increment(ref _category));
        }

        private void DoFullBackup(BackupInfo currentBkp, List<BackupInfo> prevBkps, List<BackupFileModel> localFiles)
        {
            WriteLog($"Found {prevBkps.Count} backups", ++_category);

            var prevBackupFiles = GetFilesFromPrevBackups(prevBkps);

            var copiedCount = 0;
            var linkedCount = 0;
            var filesCount = localFiles.Count;

            var svcDir = currentBkp.CreateFolders();
            currentBkp.CreateIncompleteAttribute();

            var smallFilesTarPath = Path.Combine(svcDir, "small-files.tar.gz");
            var directoriesTarPath = Path.Combine(svcDir, "dir-tree.tar.gz");

            var findHelper = new SessionFileFindHelper(currentBkp, prevBackupFiles);

            WriteLog("Backing up...", ++_category);

            var processed = 0;

            Dictionary<long, List<Tuple<BackupFileInfo, BackupInfo>>> byLength = prevBackupFiles
                .GroupBy(x => x.Item1.Length)
                .ToDictionary(x => x.Key, x => x.ToList());


            CreateDirectories(localFiles, directoriesTarPath, ref _category);

            var smallFiles = GetFilesForCompression(localFiles);
            ProcessSmallFiles(smallFiles, ref _category, smallFilesTarPath, currentBkp.AbsolutePath, findHelper, filesCount, currentBkp, ref linkedCount, ref processed);

            foreach (var localFileInfo in localFiles)
            {
                try
                {
                    var processedLocal = Interlocked.Increment(ref processed);

                    var newFile = Path.Combine(currentBkp.AbsolutePath, localFileInfo.RelativePathWin);
                    var newFileRelativeName = newFile.Replace(currentBkp.AbsolutePath, string.Empty);

                    var newDir = Path.GetDirectoryName(newFile);
                    if (newDir == null)
                    {
                        throw new InvalidOperationException("Cannot get file's directory");
                    }

                    if (!Directory.Exists(newDir))
                        Directory.CreateDirectory(newDir);

                    var existingFile = findHelper.FindByLengthAndHash(localFileInfo.FileInfo);

                    if (existingFile != null)
                    {
                        WriteLog($"[{processedLocal} of {filesCount}] {{link}} {localFileInfo.RelativePathWin} ", Interlocked.Increment(ref _category));
                        _hardLinkHelper.AddHardLinkToQueue(existingFile, newFile);
                        linkedCount++;
                    }
                    else
                    {
                        void ProgressCallback(double progress)
                        {
                            WriteLogExt($"{progress:F2} %");
                        }

                        WriteLog($"[{processedLocal} of {filesCount}] {localFileInfo.RelativePathWin} ", Interlocked.Increment(ref _category));
                        var copiedHash = HashSumHelper.CopyUnbufferedAndComputeHashAsyncXX(localFileInfo.FileInfo.FileName, newFile, ProgressCallback, _allowSimultaneousReadWrite).Result;

                        if (localFileInfo.FileInfo.FastHashStr == string.Concat(copiedHash.Select(b => $"{b:X}")))
                        {
                            copiedCount++;
                        }
                        else
                        {
                            WriteLog($"{localFileInfo.RelativePathWin} copy failed", Interlocked.Increment(ref _category));
                            System.Diagnostics.Debugger.Break();
                        }

                        new FileInfo(newFile).Attributes |= FileAttributes.ReadOnly;
                    }

                    var o = new BackupFileInfo
                    {
                        Path = newFileRelativeName,
                        Hash = localFileInfo.FileInfo.FastHashStr,
                        Length = localFileInfo.FileInfo.FileInfo.Length,
                        IsLink = existingFile != null,
                        Modified = localFileInfo.FileInfo.FileInfo.LastWriteTime,
                        Created = localFileInfo.FileInfo.FileInfo.CreationTime,
                    };

                    currentBkp.AddFile(o);
                }
                catch (Exception e)
                {
                    Console.WriteLine();
                    Console.WriteLine(e);
                    Console.WriteLine();
                }
            }
        }

        public async Task DoBackup()
        {
            await Task.Yield();

            Validate();

            _category = 0;

            var localFiles = GetFilesToBackup(ref _category);

            WriteLog("Discovering backups...", ++_category);

            var newBkpDate = DateTime.Now;
            var newBkpName = newBkpDate.ToString(BackupFolderNamingDateFormat, CultureInfo.InvariantCulture);
            var prevBkps = BackupInfo.DiscoverBackups(_destination).ToList();
            if (_backupRoots != null)
            {
                foreach (var root in _backupRoots)
                    prevBkps.AddRange(BackupInfo.DiscoverBackups(root));
            }

            var currentBkpDir = Path.Combine(_destination, newBkpName);
            var currentBkp = new BackupInfo(currentBkpDir)
            {
                DateTime = newBkpDate,
                AttributesAvailable = true,
            };

            if (_fastMode)
            {
                var lastFastBackup = prevBkps.Count == 0 ? null : prevBkps[prevBkps.Count - 1];
                if (lastFastBackup == null || !lastFastBackup.AttributesAvailable)
                {
                    WriteLog("Failed to backup fast, no compatible backup found. Please run full backup first", _category);
                    return;
                }

                DoFastBackup(lastFastBackup, currentBkp, localFiles);
            }
            else
            {
                DoFullBackup(currentBkp, prevBkps, localFiles);
            }

            try
            {
                if (_hardLinkHelper.HasItemsToProcess)
                {
                    WriteLog("Writing hardlinks to target", Interlocked.Increment(ref _category));
                    _hardLinkHelper.CreateHardLinks();
                }
            }
            finally
            {
                if (currentBkp.Files.Count > 0)
                {
                    currentBkp.WriteToDisk();
                    currentBkp.DeleteIncompleteAttribute();
                }
            }

            var log = "Backup done.";
            //if (copiedCount > 0)
            //{
            //    log += $" {copiedCount} files copied";
            //}

            //if (linkedCount > 0)
            //{
            //    log += $" {linkedCount} files linked";
            //}

            WriteLog(log, ++_category);
        }

        private List<Tuple<BackupFileInfo, BackupInfo>> GetFilesFromPrevBackups(IReadOnlyCollection<BackupInfo> prevBkps)
        {
            if (prevBkps.Count == 0)
                return new List<Tuple<BackupFileInfo, BackupInfo>>();

            WriteLog("Fast check backups...", ++_category);

            var prevBackupFiles = new List<Tuple<BackupFileInfo, BackupInfo>>();
            foreach (var backup in prevBkps)
            {
                string[] filesExists;
                try
                {
                    filesExists = _remoteFilesEnumerator(backup.AbsolutePath).ToArray();
                }
                catch
                {
                    Log?.Invoke("Failed to enumerate files fast, going slow route...", ++_category);

                    filesExists = Directory.EnumerateFiles(_destination, "*", SearchOption.AllDirectories).ToArray();
                }

                var filesExists1 = new HashSet<string>(filesExists, StringComparer.OrdinalIgnoreCase);

                var prevBackupFilesRaw = backup.Files
                    .Select(x =>
                    {
                        var fileFullPathWin = PathHelpers.NormalizePathWin(Path.Combine(backup.AbsolutePath, x.Path.TrimStart('\\', '/')));
                        var contains = filesExists1.Contains(fileFullPathWin);
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
                    var deleted = prevBackupFilesRaw.Where(x => !x.exists).ToList();
                    WriteLog($"Found {deletedCount} invalid records (file does not exist), please run health check command", ++_category);
                    foreach (var file in deleted)
                        WriteLog(file.finfo.Path, ++_category);
                }

                prevBackupFiles.AddRange(
                    prevBackupFilesRaw
                        .Where(x => x.exists)
                        .Select(x => Tuple.Create(x.finfo, backup)));
            }

            return prevBackupFiles;
        }

        private static List<BackupFileModel> GetFilesForCompression(List<BackupFileModel> localFiles)
        {
            const int maxFileSizeForCompression = 4 * 1024 * 1024;

            var smallFiles = new List<BackupFileModel>(localFiles.Count);
            for (var index = localFiles.Count - 1; index >= 0; index--)
            {
                var file = localFiles[index];
                if (file.FileInfo.FileInfo.Length < maxFileSizeForCompression || CompressibleFileExtensions.Contains(file.FileInfo.FileInfo.Extension))
                {
                    smallFiles.Add(file);
                    localFiles.RemoveAt(index);
                }
            }

            return smallFiles;
        }

        private void ProcessSmallFiles(List<BackupFileModel> smallFiles, ref int category, string smallFilesTarPath, string currentBkpDir, SessionFileFindHelper findHelper, int filesCount, BackupInfo currentBkp, ref int linkedCount, ref int processed)
        {
            if (smallFiles.Count == 0)
                return;

            WriteLog("Small files hash calculation...", ++_category);

            var cnt = smallFiles
                .AsParallel()
                .Select(x =>
                {
                    try
                    {
                        return x.FileInfo.FastHashStr;
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine(exception);
                        return "invalid hash: " + exception.Message;
                    }
                })
                .Count(x => x.StartsWith("invalid hash"));

            if (cnt > 0)
                WriteLog($"Found {cnt} invalid records", ++_category);

            WriteLog($"{smallFiles.Count} files will be transferred in a batch as tar.gz", ++_category);

            var sw = System.Diagnostics.Stopwatch.StartNew();

            bool created;
            var tmpTarPath = smallFilesTarPath + ".tmp";
            var archivedCount = 0;
            using (var tar = new TarGzHelper(tmpTarPath))
            {
                foreach (var file in smallFiles)
                {
                    try
                    {
                        var processedLocal = Interlocked.Increment(ref processed);

                        var newFileWin = Path.Combine(currentBkpDir, file.RelativePathWin);
                        var newFileRelativeName = newFileWin.Replace(currentBkpDir, string.Empty);

                        var existingFileWin = findHelper.FindByLengthAndHash(file.FileInfo);
                        if (existingFileWin != null)
                        {
                            _hardLinkHelper.AddHardLinkToQueue(existingFileWin, newFileWin);
                            linkedCount++;
                            WriteLog($"[{processedLocal} of {filesCount}] {{link}} {newFileRelativeName} to {existingFileWin}", Interlocked.Increment(ref _category));
                        }
                        else
                        {
                            var relFileName = file.RelativePathUnix;

                            using (var fl = file.FileInfo.FileInfo.OpenRead())
                            {
                                var hash = HashSumHelper.AddTarAndComputeHash(fl, relFileName, tar);
                                file.FileInfo.FastHash = hash;
                                ++archivedCount;
                            }

                            WriteLog($"[{processedLocal} of {filesCount}] {{tar}} {newFileRelativeName} ", Interlocked.Increment(ref _category));
                        }

                        var o = new BackupFileInfo
                        {
                            Path = newFileRelativeName,
                            Hash = file.FileInfo.FastHashStr,
                            Length = file.FileInfo.FileInfo.Length,
                            IsLink = existingFileWin != null,
                            Created = file.FileInfo.FileInfo.CreationTime,
                            Modified = file.FileInfo.FileInfo.LastWriteTime,
                        };
                        currentBkp.AddFile(o);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }

                created = tar.IsArchiveCreated;
            }

            sw.Stop();

            if (created)
            {
                var tarAndSendDuration = sw.Elapsed;

                WriteLog("Unpacking small files", Interlocked.Increment(ref _category));

                sw = System.Diagnostics.Stopwatch.StartNew();

                File.Move(tmpTarPath, smallFilesTarPath);
                _hardLinkHelper.UnpackTar(smallFilesTarPath);

                sw.Stop();

                WriteLog($"{archivedCount} files archived and transferred in {tarAndSendDuration:g} and unpacked in {sw.Elapsed:g}", ++_category);
            }
        }

        private void CopySmallFiles(List<BackupFileModel> smallFiles, string smallFilesTarPath, string currentBkpDir, SessionFileFindHelper findHelper, int filesCount, BackupInfo currentBkp, ref int linkedCount, ref int processed)
        {
            if (smallFiles.Count == 0)
                return;

            WriteLog("Small files hash calculation...", ++_category);

            WriteLog($"{smallFiles.Count} files will be transferred in a batch as tar.gz", ++_category);

            var sw = System.Diagnostics.Stopwatch.StartNew();

            bool created;
            var tmpTarPath = smallFilesTarPath + ".tmp";
            var archivedCount = 0;
            using (var tar = new TarGzHelper(tmpTarPath))
            {
                foreach (var file in smallFiles)
                {
                    try
                    {
                        var processedLocal = Interlocked.Increment(ref processed);

                        var newFileWin = Path.Combine(currentBkpDir, file.RelativePathWin);
                        var newFileRelativeName = newFileWin.Replace(currentBkpDir, string.Empty);

                        var relFileName = file.RelativePathUnix;

                        using (var fl = file.FileInfo.FileInfo.OpenRead())
                        {
                            var hash = HashSumHelper.AddTarAndComputeHash(fl, relFileName, tar);
                            file.FileInfo.FastHash = hash;
                            ++archivedCount;
                        }

                        WriteLog($"[{processedLocal} of {filesCount}] {{tar}} {newFileRelativeName} ", Interlocked.Increment(ref _category));

                        var o = new BackupFileInfo
                        {
                            Path = newFileRelativeName,
                            Hash = file.FileInfo.FastHashStr,
                            Length = file.FileInfo.FileInfo.Length,
                            IsLink = false,
                            Created = file.FileInfo.FileInfo.CreationTime,
                            Modified = file.FileInfo.FileInfo.LastWriteTime,
                        };
                        currentBkp.AddFile(o);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }

                created = tar.IsArchiveCreated;
            }

            sw.Stop();

            if (created)
            {
                var tarAndSendDuration = sw.Elapsed;

                WriteLog("Unpacking small files", Interlocked.Increment(ref _category));

                sw = System.Diagnostics.Stopwatch.StartNew();

                File.Move(tmpTarPath, smallFilesTarPath);
                _hardLinkHelper.UnpackTar(smallFilesTarPath);

                sw.Stop();

                WriteLog($"{archivedCount} files archived and transferred in {tarAndSendDuration:g} and unpacked in {sw.Elapsed:g}", ++_category);
            }
        }

        private void CreateDirectories(List<BackupFileModel> files, string tarPath, ref int category)
        {
            var dirList = files
                .Select(x => x.RelativeDirectoryNameUnix)
                .Distinct()
                .Where(x => !string.IsNullOrEmpty(x))
                .ToList();

            if (dirList.Count == 0)
                return;

            WriteLog("Creating directories...", ++_category);

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

            var tmpTarPath = tarPath + ".tmp";
            using (var tar = new TarGzHelper(tmpTarPath))
            {
                dirList.ForEach(tar.AddEmptyFolder);
            }

            File.Move(tmpTarPath, tarPath);

            _hardLinkHelper.UnpackTar(tarPath);
        }

        private void Validate()
        {
            if (_sources.Any(x => !Directory.Exists(x.FullPath)))
            {
                throw new InvalidOperationException("Source directory does not exist");
            }

            if (!Directory.Exists(_destination))
            {
                throw new InvalidOperationException("Destination directory does not exist");
            }
        }

        /*
        private static void CreatePar(string file, BackupInfo currentBkp)
        {
            var parFile = currentBkp.AbsolutePath + $"\\.bkp\\par{file.Replace(currentBkp.AbsolutePath, null)}.par";
            var proc = new System.Diagnostics.Process
            {
                StartInfo = new System.Diagnostics.ProcessStartInfo
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
        */
    }
}