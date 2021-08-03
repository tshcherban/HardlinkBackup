using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Backuper;

namespace HardLinkBackup
{
    public static class HashSumHelper
    {
        private const int BufferSizeMib = 4;
        private const int BuffersCount = 4;

        public static async Task<byte[]> CopyUnbufferedAndComputeHashAsyncXX(string filePath, string destinationPath, Action<double> progressCallback, bool allowSimultaneousIo)
        {
            const FileOptions fileFlagNoBuffering = (FileOptions) 0x20000000;
            const FileOptions fileOptions = fileFlagNoBuffering | FileOptions.SequentialScan;

            const int chunkSize = BufferSizeMib * 1024 * 1024;

            var readBufferSize = chunkSize;
            readBufferSize += ((readBufferSize + 1023) & ~1023) - readBufferSize;

            var folder = Path.GetDirectoryName(filePath);
            if (folder == null)
            {
                throw new InvalidOperationException($"Failed to get file '{filePath}' folder");
            }

            if (!Directory.Exists(folder))
            {
                Directory.CreateDirectory(folder);
            }

            using (var sourceStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fileOptions))
            using (var bufferedSourceStream = new BufferedStream(sourceStream, readBufferSize))
            using (var newFileStream = new FileStream(destinationPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, readBufferSize, FileOptions.WriteThrough | FileOptions.Asynchronous | fileFlagNoBuffering))
            {
                var fileLength = sourceStream.Length;
                if (fileLength == 0)
                {
                    return XxHash64Callback.EmptyHash;
                }

                async Task WriteToFile(byte[] bytes, int length)
                {
                    await newFileStream.WriteAsync(bytes, 0, length);
                }

                return await XxHash64Callback.ComputeHash(bufferedSourceStream, chunkSize, fileLength, WriteToFile);
            }
        }

        public static byte[] AddTarAndComputeHash(Stream file, string relativeFileName, TarGzHelper tar)
        {
            const int chunkSize = BufferSizeMib * 1024 * 1024;

            var fileLength = file.Length;

            tar.BeginAddFile(relativeFileName, fileLength);

            Task WriteToFile(byte[] bytes, int length)
            {
                tar.WriteFileContent(bytes, length);
                return Task.CompletedTask;
            }

            var hash = fileLength == 0
                ? XxHash64Callback.EmptyHash
                : XxHash64Callback.ComputeHash(file, chunkSize, fileLength, WriteToFile).GetAwaiter().GetResult();

            tar.EndAddFile(fileLength);

            return hash;
        }
    }
}