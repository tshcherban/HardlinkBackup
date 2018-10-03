using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Backuper
{
    public class RepositoryHelper
    {
        private readonly BackupDatabase _db;
        private readonly string _repoDir;

        public RepositoryHelper(BackupDatabase db, string repoDir)
        {
            _db = db;
            _repoDir = repoDir;
        }

        public async Task<RepositoryItemWrap> GetOrAdd(string hashStr, string filePath, long length)
        {
            if (_db.Repository.TryGetValue(length, out var byHash) && byHash.TryGetValue(hashStr, out var item))
            {
                return new RepositoryItemWrap {Item = item, IsFromRepo = true};
            }

            var fName = GetFileName();

            const int chunkSize = 4 * 1024 * 1024;

            var readBufferSize = chunkSize;
            readBufferSize += ((readBufferSize + 1023) & ~1023) - readBufferSize;

            const FileOptions fileFlagNoBuffering = (FileOptions)0x20000000;
            const FileOptions fileOptions = fileFlagNoBuffering | FileOptions.SequentialScan;

                using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fileOptions))
                using (var bufferedStream = new BufferedStream(fileStream, readBufferSize))
                using (var destinationStream = new FileStream(fName, FileMode.CreateNew, FileAccess.Write, FileShare.None, readBufferSize, FileOptions.WriteThrough))
                //using (var destinationStream1 = new GZipStream(destinationStream, CompressionLevel.Fastest))
                {
                    //await bufferedStream.CopyToAsync(destinationStream1);
                    await bufferedStream.CopyToAsync(destinationStream);
                }

//            File.Copy(filePath, fName);

            item = new RepositoryItem
            {
                HashStr = hashStr,
                RelativePath = fName.Replace(_repoDir, null).TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar),
                Length = length
            };

            if (byHash == null)
            {
                byHash = new Dictionary<string, RepositoryItem>();
                _db.Repository[length] = byHash;
            }

            byHash[hashStr] = item;

            return new RepositoryItemWrap {Item = item, IsFromRepo = false};
        }

        private string GetFileName()
        {
            if (!Directory.Exists(_repoDir))
            {
                Directory.CreateDirectory(_repoDir);
            }

            string dir;

            var dirs = Directory.GetDirectories(_repoDir);
            if (dirs.Length == 0)
            {
                dir = Directory.CreateDirectory(Path.Combine(_repoDir, "1")).FullName;
            }
            else
            {
                dir = dirs.Last();

                if (Directory.GetFiles(dir).Length > 3000)
                {
                    var newNumber = int.Parse(dir.Replace(_repoDir, null).Trim(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)) + 1;
                    dir = Directory.CreateDirectory(Path.Combine(_repoDir, newNumber.ToString())).FullName;
                }
            }

            string fName;

            var files = Directory.GetFiles(dir);
            if (files.Length == 0)
            {
                fName = "1";
            }
            else
            {
                fName = (files
                             .Select(x => x.Replace(dir, null).Trim(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))
                             .Select(int.Parse)
                             .Max() + 1)
                    .ToString();
            }

            fName = Path.Combine(dir, fName);
            return fName;
        }
    }

    public sealed class BackupEngine
    {
        private readonly string _sourceDir;
        private readonly string _targetDir;
        private readonly string _backupDb;
        private RepositoryHelper _repositoryHandler;

        private BackupDatabase _db;

        public BackupEngine(string source, string target)
        {
            _sourceDir = source;
            _targetDir = target;
            _backupDb = Path.Combine(_targetDir, ".database.json");
        }

        public async Task<long> DoBackup()
        {
            _db = ReadDb();

            var localFileInfos = await ReadLocal();

            _repositoryHandler = new RepositoryHelper(_db, Path.Combine(_targetDir, ".repository"));

            var bkp = new BackupItem
            {
                BackupDate = DateTime.Now,
            };

            var sw = Stopwatch.StartNew();

            var processed = 0;

            Console.WriteLine("Writing repo...");

            var repoHitCount = 0;
            var newItemsCount = 0;
            long bytesSaved = 0;

            var updateLogTask = Task.Run(() =>
            {
                var p1 = 0;
                while (processed < localFileInfos.Count)
                {
                    Console.CursorLeft = 0;

                    var processSpeed = processed / sw.Elapsed.TotalSeconds;
                    var mbSaved = bytesSaved / 1024m / 1024m;
                    Console.Write($"{processed} of {localFileInfos.Count} done ({repoHitCount} hardlinked, {mbSaved:F2} mb saved), {processSpeed:F1} items/s");
                    Task.Delay(250).Wait();
                }
            });

            foreach (var localFileInfo in localFileInfos)
            {
                try
                {
                    var filePath = Path.Combine(_sourceDir, localFileInfo.RelativePath);
                    var repoItem = await _repositoryHandler.GetOrAdd(localFileInfo.HashStr, filePath, localFileInfo.Length);
                    if (repoItem.IsFromRepo)
                    {
                        repoHitCount++;
                        bytesSaved += repoItem.Item.Length;
                    }
                    else
                    {
                        newItemsCount++;
                    }

                    bkp.Files.Add(new BackupFile
                    {
                        RelativePath = localFileInfo.RelativePath,
                        Item = repoItem.Item,
                    });
                }
                catch
                {

                }

                Interlocked.Increment(ref processed);
            }

            sw.Stop();

            await updateLogTask;

            var speed = localFileInfos.Sum(x => x.Length) / (decimal) sw.Elapsed.TotalSeconds / 1024m / 1024m;

            Console.WriteLine($"Done in {sw.Elapsed.TotalSeconds:F2} s ({speed:F2} mb/s), repo items {repoHitCount}, new items {newItemsCount}");

            _db.Backups.Add(bkp);

            var str = JsonConvert.SerializeObject(_db, Formatting.Indented, new JsonSerializerSettings
            {
                PreserveReferencesHandling = PreserveReferencesHandling.Objects,
            });

            File.WriteAllText(_backupDb, str);

            return localFileInfos.Sum(x => x.Length);
        }

        private List<BackupFileInfo> GetChangedFiles(List<BackupFileInfo> localFileInfos, List<BackupFileInfo> backupFileInfos)
        {
            if (backupFileInfos.Count == 0)
            {
                return localFileInfos;
            }

            var backupFileInfosLookup = backupFileInfos.ToDictionary(x => x.RelativePath);

            return localFileInfos
                .Where(localFile => !backupFileInfosLookup.TryGetValue(localFile.RelativePath, out var backupFile) || localFile.HashStr != backupFile.HashStr)
                .ToList();
        }

        private BackupDatabase ReadDb()
        {
            if (!File.Exists(_backupDb))
            {
                return new BackupDatabase();
            }

            var str = File.ReadAllText(_backupDb);
            return JsonConvert.DeserializeObject<BackupDatabase>(str);
        }

        private async Task<List<BackupFileInfo>> ReadBackup()
        {
            _db = ReadDb();

            if (_db.Backups.Count == 0)
            {
                return new List<BackupFileInfo>();
            }

            var latestBackup = _db.Backups.OrderByDescending(x => x.BackupDate).First();

            return latestBackup.Files
                .Select(x => new BackupFileInfo
                {
                    RelativePath = x.RelativePath,
                    HashStr = x.Item.HashStr,
                    Length = x.Item.Length,
                })
                .ToList();
        }

        private async Task<List<BackupFileInfo>> ReadLocal()
        {
            var files = Directory.GetFiles(_sourceDir, "*", SearchOption.AllDirectories);

            var processed = 0;

            var sw = Stopwatch.StartNew();

            var updateLogTask = Task.Run(() =>
            {
                while (processed < files.Length)
                {
                    Console.CursorLeft = 0;

                    var speed = processed / sw.Elapsed.TotalSeconds;
                    Console.Write($"{processed} of {files.Length} done, {speed:F1} items/s");
                    Task.Delay(500).Wait();
                }

                Console.CursorLeft = 0;
                Console.WriteLine();
            });

            Console.WriteLine("Reading local copy...");
            Console.WriteLine();

            var tasks = files
                .AsParallel()
                .Select(async file =>
                {
                    try
                    {
                        Interlocked.Increment(ref processed);

                        var fi = new FileInfo(file);
                        var hashByte = await HashHelper.HashFileAsync(fi);
                        var hash = hashByte.ToHashString();
                        var bif = new BackupFileInfo
                        {
                            RelativePath = file.Replace(_sourceDir, null).TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar),
                            HashStr = hash,
                            Length = fi.Length,
                        };

                        return bif;
                    }
                    catch
                    {
                        return null;
                    }

                })
                .Where(x => x != null)
                .ToList();

            await Task.WhenAll(tasks);

            await updateLogTask;

            sw.Stop();

            var fileInfos = tasks.Select(x => x.Result).Where(x => x != null).ToList();

            return fileInfos;
        }
    }

    public sealed class BackupDatabase
    {
        public Dictionary<long, Dictionary<string, RepositoryItem>> Repository { get; set; } = new Dictionary<long, Dictionary<string, RepositoryItem>>();

        public List<BackupItem> Backups { get; set; } = new List<BackupItem>();
    }

    public sealed class BackupItem
    {
        public DateTime BackupDate { get; set; }

        public List<BackupFile> Files { get; set; } = new List<BackupFile>();
    }

    public sealed class BackupFile
    {
        public string RelativePath { get; set; }

        public RepositoryItem Item { get; set; }
    }

    public sealed class RepositoryItemWrap
    {
        public RepositoryItem Item { get; set; }

        public bool IsFromRepo { get; set; }
    }

    public sealed class RepositoryItem
    {
        public int Id { get; set; }

        public string RelativePath { get; set; }

        public string HashStr { get; set; }
        
        public long Length { get; set; }
    }

    public sealed class BackupFileInfo
    {
        public string HashStr { get; set; }

        public string RelativePath { get; set; }
        
        public long Length { get; set; }
    }

    public static class Extensions
    {
        public static string ToHashString(this byte[] array)
        {
            var sb = new StringBuilder();
            for (var i = 0; i < array.Length; ++i)
            {
                sb.Append(array[i].ToString("x2"));
            }

            return sb.ToString();
        }
    }
}