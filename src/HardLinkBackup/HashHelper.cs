using System.IO;
using System.Threading.Tasks;
using HardLinkBackup;

namespace Backuper
{
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
                return await XxHash64Callback.ComputeHash(bufferedStream, ChunkSize, fileLength, (bytes, i) => Task.CompletedTask);
            }
        }
    }
}