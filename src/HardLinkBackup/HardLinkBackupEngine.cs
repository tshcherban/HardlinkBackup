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

        public event Action<string, int> Log;

        public event Action<string> LogExt;

        public HardLinkBackupEngine(string rootDir, string[] sources, string[] backupRoots, string destination, bool allowSimultaneousReadWrite, IHardLinkHelper hardLinkHelper, Func<string, string[]> remoteFilesEnumerator)
        {
            //_rootDir = rootDir.EndsWith(@":\\") ? rootDir : rootDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _destination = destination.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _sources = GetSources(rootDir, sources);
            _backupRoots = backupRoots;
            _allowSimultaneousReadWrite = allowSimultaneousReadWrite;
            _hardLinkHelper = hardLinkHelper;
            _remoteFilesEnumerator = remoteFilesEnumerator;
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
            WriteLog("Enumerating files...", ++category);

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
                WriteLog($"{invalidFiles.Count} files have paths with part longer than 250. They will not be copied", ++category);
                foreach (var f in invalidFiles)
                {
                    WriteLog(f.RelativePathWin, ++category);
                    files.Remove(f);
                }
            }

            return files;
        }

        public async Task DoBackup()
        {
            await Task.Yield();

            Validate();

            var category = 0;

            var localFiles = GetFilesToBackup(ref category);

            WriteLog("Discovering backups...", ++category);

            var newBkpDate = DateTime.Now;
            var newBkpName = newBkpDate.ToString(BackupFolderNamingDateFormat, CultureInfo.InvariantCulture);
            var prevBkps = BackupInfo.DiscoverBackups(_destination).ToList();
            if (_backupRoots != null)
            {
                foreach (var root in _backupRoots)
                    prevBkps.AddRange(BackupInfo.DiscoverBackups(root));
            }

            WriteLog($"Found {prevBkps.Count} backups", ++category);

            var currentBkpDir = Path.Combine(_destination, newBkpName);
            var filesCount = localFiles.Count;
            var currentBkp = new BackupInfo(currentBkpDir)
            {
                DateTime = newBkpDate,
            };

            var prevBackupFiles = GetFilesFromPrevBackups(prevBkps, ref category);

            var copiedCount = 0;
            var linkedCount = 0;

            var svcDir = currentBkp.CreateFolders();
            currentBkp.CreateIncompleteAttribute();

            var smallFilesTarPath = Path.Combine(svcDir, "small-files.tar.gz");
            var directoriesTarPath = Path.Combine(svcDir, "dir-tree.tar.gz");

            var findHelper = new SessionFileFindHelper(currentBkp, prevBackupFiles);

            CreateDirectories(localFiles, directoriesTarPath, ref category);

            WriteLog("Backing up...", ++category);

            var processed = 0;

            var smallFiles = GetFilesForCompression(localFiles);
            ProcessSmallFiles(smallFiles, ref category, smallFilesTarPath, currentBkpDir, findHelper, filesCount, currentBkp, ref linkedCount, ref processed);

            foreach (var localFileInfo in localFiles)
            {
                try
                {
                    var processedLocal = Interlocked.Increment(ref processed);

                    var newFile = Path.Combine(currentBkpDir, localFileInfo.RelativePathWin);
                    var newFileRelativeName = newFile.Replace(currentBkpDir, string.Empty);

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
                        WriteLog($"[{processedLocal} of {filesCount}] {{link}} {localFileInfo.RelativePathWin} ", Interlocked.Increment(ref category));
                        _hardLinkHelper.AddHardLinkToQueue(existingFile, newFile);
                        linkedCount++;
                    }
                    else
                    {
                        void ProgressCallback(double progress)
                        {
                            WriteLogExt($"{progress:F2} %");
                        }

                        WriteLog($"[{processedLocal} of {filesCount}] {localFileInfo.RelativePathWin} ", Interlocked.Increment(ref category));
                        var copiedHash = HashSumHelper.CopyUnbufferedAndComputeHashAsyncXX(localFileInfo.FileInfo.FileName, newFile, ProgressCallback, _allowSimultaneousReadWrite).Result;

                        if (localFileInfo.FileInfo.FastHashStr == string.Concat(copiedHash.Select(b => $"{b:X}")))
                        {
                            copiedCount++;
                        }
                        else
                        {
                            WriteLog($"{localFileInfo.RelativePathWin} copy failed", Interlocked.Increment(ref category));
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

            WriteLog("Writing hardlinks to target", Interlocked.Increment(ref category));

            try
            {
                _hardLinkHelper.CreateHardLinks();
            }
            finally
            {
                currentBkp.WriteToDisk();
                currentBkp.DeleteIncompleteAttribute();
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

        private List<Tuple<BackupFileInfo, BackupInfo>> GetFilesFromPrevBackups(List<BackupInfo> prevBkps, ref int category)
        {
            if (prevBkps.Count == 0)
                return new List<Tuple<BackupFileInfo, BackupInfo>>();

            WriteLog("Fast check backups...", ++category);

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
                    Log?.Invoke("Failed to enumerate files fast, going slow route...", ++category);

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
                    WriteLog($"Found {deletedCount} invalid records (file does not exist), please run health check command", ++category);
                    foreach (var file in deleted)
                        WriteLog(file.finfo.Path, ++category);
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

            WriteLog("Small files hash calculation...", ++category);

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
                WriteLog($"Found {cnt} invalid records", ++category);

            WriteLog($"{smallFiles.Count} files will be transferred in a batch as tar.gz", ++category);

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
                            WriteLog($"[{processedLocal} of {filesCount}] {{link}} {newFileRelativeName} to {existingFileWin}", Interlocked.Increment(ref category));
                        }
                        else
                        {
                            var relFileName = file.RelativePathUnix;
                            using (var fl = file.FileInfo.FileInfo.OpenRead())
                            {
                                tar.AddFile(relFileName, fl);
                                ++archivedCount;
                            }

                            WriteLog($"[{processedLocal} of {filesCount}] {{tar}} {newFileRelativeName} ", Interlocked.Increment(ref category));
                        }

                        var o = new BackupFileInfo
                        {
                            Path = newFileRelativeName,
                            Hash = file.FileInfo.FastHashStr,
                            Length = file.FileInfo.FileInfo.Length,
                            IsLink = existingFileWin != null,
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

                WriteLog("Unpacking small files", Interlocked.Increment(ref category));

                sw = System.Diagnostics.Stopwatch.StartNew();

                File.Move(tmpTarPath, smallFilesTarPath);
                _hardLinkHelper.UnpackTar(smallFilesTarPath);

                sw.Stop();

                WriteLog($"{archivedCount} files archived and transferred in {tarAndSendDuration:g} and unpacked in {sw.Elapsed:g}", ++category);
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