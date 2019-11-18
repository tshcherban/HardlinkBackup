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

        //private readonly string _rootDir;
        private readonly string _destination;
        private readonly FilesSource[] _sources;
        private readonly bool _allowSimultaneousReadWrite;
        private readonly IHardLinkHelper _hardLinkHelper;
        private readonly Func<string[]> _remoteFilesEnumerator;

        public event Action<string, int> Log;

        public event Action<string> LogExt;

        public HardLinkBackupEngine(string rootDir, string[] sources, string destination, bool allowSimultaneousReadWrite, IHardLinkHelper hardLinkHelper, Func<string[]> remoteFilesEnumerator)
        {
            //_rootDir = rootDir.EndsWith(@":\\") ? rootDir : rootDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _destination = destination.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            _sources = GetSources(rootDir, sources);
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


        private static readonly HashSet<string> CompressibleFileExtensions = new HashSet<string>
        {
            ".xml",
            ".txt",
            ".pdb",
            ".dll",
            ".log",
        };

        public static IEnumerable<string> GetDirectoryFiles(string rootPath, string patternMatch, SearchOption searchOption)
        {
            var foundFiles = Enumerable.Empty<string>();

            if (searchOption == SearchOption.AllDirectories)
            {
                try
                {
                    IEnumerable<string> subDirs = Directory.EnumerateDirectories(rootPath);
                    foreach (string dir in subDirs)
                    {
                        foundFiles = foundFiles.Concat(GetDirectoryFiles(dir, patternMatch, searchOption)); // Add files in subdirectories recursively to the list
                    }
                }
                catch (UnauthorizedAccessException) { }
                catch (PathTooLongException) {}
            }

            try
            {
                foundFiles = foundFiles.Concat(Directory.EnumerateFiles(rootPath, patternMatch)); // Add files from the current directory
            }
            catch (UnauthorizedAccessException) { }

            return foundFiles;
        }

        private List<BackupFileModel> GetFilesToBackup(ref int category)
        {
            WriteLog("Enumerating files...", ++category);

            var files = new List<BackupFileModel>();

            foreach (var src in _sources)
            {
                var fil = GetDirectoryFiles(src.FullPath, "*", SearchOption.AllDirectories)
                    .Select(fullFileName =>
                    {
                        try
                        {
                            var relativeFileName = fullFileName.Replace(src.FullPath, src.Alias);

                            return new BackupFileModel(fullFileName, relativeFileName);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
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
                    files.Remove(f);
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

                    var existingFile = findHelper.FindFile(localFileInfo.FileInfo);

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

            string[] filesExists;
            try
            {
                filesExists = _remoteFilesEnumerator().Select(x => PathHelpers.NormalizePathWin(x)).ToArray();
            }
            catch
            {
                Log?.Invoke("Failed to enumerate files fast, going slow route...", ++category);

                filesExists = Directory.EnumerateFiles(_destination, "*", SearchOption.AllDirectories).ToArray();
            }

            var filesExists1 = new HashSet<string>(filesExists, StringComparer.OrdinalIgnoreCase);

            var prevBackupFilesRaw = prevBkps
                .SelectMany(b => b.Files.Select(fll => new {file = fll, backup = b}))
                .Select(x =>
                {
                    var contains = filesExists1.Contains(PathHelpers.NormalizePathWin(x.backup.AbsolutePath + x.file.Path));
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
            using (var tar = new TarGzHelper(tmpTarPath))
            {
                foreach (var file in smallFiles)
                {
                    try
                    {
                        var newFile = Path.Combine(currentBkpDir, file.RelativePathWin);
                        var newFileRelativeName = newFile.Replace(currentBkpDir, string.Empty);

                        var existingFile = findHelper.FindFile(file.FileInfo);
                        if (existingFile != null)
                        {
                            _hardLinkHelper.AddHardLinkToQueue(existingFile, newFile);
                            linkedCount++;

                            WriteLog($"[{Interlocked.Increment(ref processed)} of {filesCount}] {{link}} {newFileRelativeName} ", Interlocked.Increment(ref category));
                        }
                        else
                        {
                            var relFileName = file.RelativePathUnix;
                            using (var fl = file.FileInfo.FileInfo.OpenRead())
                                tar.AddFile(relFileName, fl);

                            WriteLog($"[{Interlocked.Increment(ref processed)} of {filesCount}] {{tar}} {newFileRelativeName} ", Interlocked.Increment(ref category));
                        }

                        var o = new BackupFileInfo
                        {
                            Path = newFileRelativeName,
                            Hash = file.FileInfo.FastHashStr,
                            Length = file.FileInfo.FileInfo.Length,
                            IsLink = existingFile != null,
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

                WriteLog($"{smallFiles.Count} files archived and transferred in {tarAndSendDuration:g} and unpacked in {sw.Elapsed:g}", ++category);
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
    }

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

    public class FilesSource
    {
        public FilesSource(string fullPath, string alias)
        {
            FullPath = fullPath;
            Alias = alias;
        }

        public string FullPath { get; }

        public string Alias { get; }
    }

    public static class PathHelpers
    {
        public static string NormalizePathWin(string path, char separator = '\\')
        {
            return path?.Replace('/', separator).Replace('\\', separator);
        }

        public static string NormalizePathUnix(string path, char separator = '/')
        {
            if (string.IsNullOrEmpty(path))
                return path;

            if ((path.StartsWith(@":\") || path.StartsWith(@":/")) && char.IsLetter(path[0]))
                throw new Exception("Unix path shouldn't start from windows drive name");

            return path.Replace('\\', separator).Replace('/', separator);
        }
    }
}