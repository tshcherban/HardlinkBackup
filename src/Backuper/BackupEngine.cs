using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Policy;
using System.Text;
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

        public async Task<RepositoryItem> GetOrAdd(string hashStr, string filePath, long length)
        {
            if (_db.Repository.TryGetValue(length, out var byHash) && byHash.TryGetValue(hashStr, out var item))
            {
                return item;
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
            };

            if (byHash == null)
            {
                byHash = new Dictionary<string, RepositoryItem>();
                _db.Repository[length] = byHash;
            }

            byHash[hashStr] = item;

            return item;
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
            var localFileInfos = await ReadLocal();
            var backupFileInfos = await ReadBackup();

            _repositoryHandler = new RepositoryHelper(_db, Path.Combine(_targetDir, ".repository"));

            var bkp = new BackupItem
            {
                BackupDate = DateTime.Now,
            };

            var sw = Stopwatch.StartNew();

            foreach (var localFileInfo in localFileInfos)
            {
                var filePath = Path.Combine(_sourceDir, localFileInfo.RelativePath);
                var repoItem = await _repositoryHandler.GetOrAdd(localFileInfo.HashStr, filePath, localFileInfo.Length);
                bkp.Files.Add(new BackupFile
                {
                    RelativePath = localFileInfo.RelativePath,
                    Item = repoItem,
                });
            }

            sw.Stop();

            var speed = localFileInfos.Sum(x => x.Length) / (decimal) sw.Elapsed.TotalSeconds / 1024m / 1024m;

            Console.WriteLine($"Done in {sw.Elapsed.TotalSeconds:F2} s ({speed:F2} mb/s)");

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

        private async Task<List<BackupFileInfo>> ReadBackup()
        {
            if (!File.Exists(_backupDb))
            {
                _db = new BackupDatabase();

                return new List<BackupFileInfo>();
            }

            var str = File.ReadAllText(_backupDb);
            _db = JsonConvert.DeserializeObject<BackupDatabase>(str);

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
            var tasks = files
                .Select(async file =>
                {
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
                })
                .ToList();

            await Task.WhenAll(tasks);

            var fileInfos = tasks.Select(x => x.Result).ToList();

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

    public static class HashHelper
    {
        private const int ChunkSize = 4 * 1024 * 1024;
        private const int ReadBufferSize = ChunkSize + ((ChunkSize + 1023) & ~1023) - ChunkSize;

        private const FileOptions FileFlagNoBuffering = (FileOptions) 0x20000000;
        private const FileOptions FileOptions = FileFlagNoBuffering | System.IO.FileOptions.SequentialScan;

        public static async Task<byte[]> HashFileAsync(string filePath)
        {
            return await HashFileAsync(new FileInfo(filePath));
        }

        public static async Task<byte[]> HashFileAsync(FileInfo inf)
        {
            return await HashFileAsync(inf.FullName, inf.Length);
        }

        private static async Task<byte[]> HashFileAsync(string filePath, long fileLength)
        {
            using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, ReadBufferSize, FileOptions))
            using (var bufferedStream = new BufferedStream(fileStream, ReadBufferSize))
            {
                return await XxHash64Callback.ComputeHash(bufferedStream, ChunkSize, fileLength);
            }
        }
    }

    public static class XxHash64Callback
    {
        public static byte[] EmptyHash = new byte[sizeof(ulong)];

        private const int Min64 = 1024;
        private const int Div32 = 0x7FFFFFE0;

        private const ulong P1 = 11400714785074694791UL;
        private const ulong P2 = 14029467366897019727UL;
        private const ulong P3 = 1609587929392839161UL;
        private const ulong P4 = 9650029242287828579UL;
        private const ulong P5 = 2870177450012600261UL;

        public static async Task<byte[]> ComputeHash(Stream stream, int bufferSize, long length)
        {
            if (length == 0)
            {
                return EmptyHash;
            }

            // The buffer can't be less than 1024 bytes
            if (bufferSize < Min64)
            {
                bufferSize = Min64;
            }
            else
            {
                bufferSize &= Div32;
            }

            // Calculate the number of chunks and the remain
            var chunks = length / bufferSize;
            var remain = length % bufferSize;
            var offset = bufferSize;

            // Calculate the offset
            if (remain != 0) chunks++;
            if (remain != 0 && remain < 32) offset -= 32;

            var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

            try
            {
                return await HashCore(stream, bufferSize, chunks, offset, buffer, length);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private static async Task ReadExact(Stream stream, byte[] buffer, int count)
        {
            var left = count;
            var read = 0;
            while (left > 0)
            {
                read += await stream.ReadAsync(buffer, read, left);
                left = count - read;
            }
        }

        private static async Task<byte[]> HashCore(Stream stream, int bufferSize, long chunks, int offset, byte[] buffer, long length)
        {
            // Prepare the seed vector
            var v1 = unchecked(P1 + P2);
            var v2 = P2;
            var v3 = 0ul;
            var v4 = P1;

            long read = 0;

            // Process chunks
            // Skip the last chunk. It will processed a little bit later
            for (var i = 2L; i <= chunks; i++)
            {
                // Change bufferSize for the last read
                if (i == chunks)
                {
                    bufferSize = offset;
                }

                // Read the next chunk
                await ReadExact(stream, buffer, bufferSize);

                read += bufferSize;

                unsafe
                {
                    fixed (byte* pData = &buffer[0])
                    {
                        var ptr = pData;
                        var end = pData + bufferSize;

                        do
                        {
                            v1 += *((ulong*) ptr) * P2;
                            v1 = (v1 << 31) | (v1 >> (64 - 31)); // rotl 31
                            v1 *= P1;
                            ptr += 8;

                            v2 += *((ulong*) ptr) * P2;
                            v2 = (v2 << 31) | (v2 >> (64 - 31)); // rotl 31
                            v2 *= P1;
                            ptr += 8;

                            v3 += *((ulong*) ptr) * P2;
                            v3 = (v3 << 31) | (v3 >> (64 - 31)); // rotl 31
                            v3 *= P1;
                            ptr += 8;

                            v4 += *((ulong*) ptr) * P2;
                            v4 = (v4 << 31) | (v4 >> (64 - 31)); // rotl 31
                            v4 *= P1;
                            ptr += 8;
                        } while (ptr < end);
                    }
                }
            }

            // Read the last chunk
            //offset = stream.Read(buffer, 0, bufferSize);

            var toRead = length - read;
            var toReadInt = (int) toRead;

            await ReadExact(stream, buffer, toReadInt);

            ulong h64;

            unsafe
            {
                // Process the last chunk
                fixed (byte* pData = &buffer[0])
                {
                    var ptr = pData;
                    var end = pData + toReadInt;

                    if (length >= 32)
                    {
                        var limit = end - 32;

                        do
                        {
                            v1 += *((ulong*) ptr) * P2;
                            v1 = (v1 << 31) | (v1 >> (64 - 31)); // rotl 31
                            v1 *= P1;
                            ptr += 8;

                            v2 += *((ulong*) ptr) * P2;
                            v2 = (v2 << 31) | (v2 >> (64 - 31)); // rotl 31
                            v2 *= P1;
                            ptr += 8;

                            v3 += *((ulong*) ptr) * P2;
                            v3 = (v3 << 31) | (v3 >> (64 - 31)); // rotl 31
                            v3 *= P1;
                            ptr += 8;

                            v4 += *((ulong*) ptr) * P2;
                            v4 = (v4 << 31) | (v4 >> (64 - 31)); // rotl 31
                            v4 *= P1;
                            ptr += 8;
                        } while (ptr <= limit);

                        h64 = ((v1 << 1) | (v1 >> (64 - 1))) + // rotl 1
                              ((v2 << 7) | (v2 >> (64 - 7))) + // rotl 7
                              ((v3 << 12) | (v3 >> (64 - 12))) + // rotl 12
                              ((v4 << 18) | (v4 >> (64 - 18))); // rotl 18

                        // merge round
                        v1 *= P2;
                        v1 = (v1 << 31) | (v1 >> (64 - 31)); // rotl 31
                        v1 *= P1;
                        h64 ^= v1;
                        h64 = h64 * P1 + P4;

                        // merge round
                        v2 *= P2;
                        v2 = (v2 << 31) | (v2 >> (64 - 31)); // rotl 31
                        v2 *= P1;
                        h64 ^= v2;
                        h64 = h64 * P1 + P4;

                        // merge round
                        v3 *= P2;
                        v3 = (v3 << 31) | (v3 >> (64 - 31)); // rotl 31
                        v3 *= P1;
                        h64 ^= v3;
                        h64 = h64 * P1 + P4;

                        // merge round
                        v4 *= P2;
                        v4 = (v4 << 31) | (v4 >> (64 - 31)); // rotl 31
                        v4 *= P1;
                        h64 ^= v4;
                        h64 = h64 * P1 + P4;
                    }
                    else
                    {
                        h64 = P5;
                    }

                    h64 += (ulong) length;

                    // finalize
                    while (ptr <= end - 8)
                    {
                        var t1 = *((ulong*) ptr) * P2;
                        t1 = (t1 << 31) | (t1 >> (64 - 31)); // rotl 31
                        t1 *= P1;
                        h64 ^= t1;
                        h64 = ((h64 << 27) | (h64 >> (64 - 27))) * P1 + P4; // (rotl 27) * p1 + p4
                        ptr += 8;
                    }

                    if (ptr <= end - 4)
                    {
                        h64 ^= *((uint*) ptr) * P1;
                        h64 = ((h64 << 23) | (h64 >> (64 - 23))) * P2 + P3; // (rotl 27) * p2 + p3
                        ptr += 4;
                    }

                    while (ptr < end)
                    {
                        h64 ^= *((byte*) ptr) * P5;
                        h64 = ((h64 << 11) | (h64 >> (64 - 11))) * P1; // (rotl 11) * p1
                        ptr += 1;
                    }

                    // avalanche
                    h64 ^= h64 >> 33;
                    h64 *= P2;
                    h64 ^= h64 >> 29;
                    h64 *= P3;
                    h64 ^= h64 >> 32;
                }
            }

            return BitConverter.GetBytes(h64);
        }
    }
}