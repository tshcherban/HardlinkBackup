using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace HardLinkBackup
{
    public static class HashSumHelper
    {
        private const int BufferSizeMib = 32;
        private const int BuffersCount = 4;

        public static async Task<byte[]> CopyUnbufferedAndComputeHashAsync(string filePath, string destinationPath, Action<double> progressCallback, bool allowSimultaneousIo)
        {
            const FileOptions fileFlagNoBuffering = (FileOptions)0x20000000;
            const FileOptions fileOptions = fileFlagNoBuffering | FileOptions.SequentialScan;

            const int chunkSize = BufferSizeMib * 1024 * 1024;

            var readBufferSize = chunkSize;
            readBufferSize += ((readBufferSize + 1023) & ~1023) - readBufferSize;

            using (HashAlgorithm hashAlgorithm = SHA1.Create())
            using (var sourceStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fileOptions))
            using (var destinationStream = new FileStream(destinationPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, readBufferSize, FileOptions.WriteThrough))
            {
                var length = sourceStream.Length;

                var readSize = Convert.ToInt32(Math.Min(chunkSize, length));

                var buffer = new LightBuffer[BuffersCount];
                for (var i = 0; i < BuffersCount; ++i)
                    buffer[i] = new LightBuffer(readSize);

                var ioLock = new AsyncLock();

                var readTask = GetReadTask(length, buffer, allowSimultaneousIo, sourceStream, readSize, ioLock);

                var progress = readSize < length
                    ? new Action<long>(done => progressCallback?.Invoke(done / (double) length * 100d))
                    : null;
                var writeTask = GetWriteTask(buffer, hashAlgorithm, allowSimultaneousIo, ioLock, destinationStream, progress);

                await Task.WhenAll(readTask, writeTask);

                return hashAlgorithm.Hash;
            }
        }

        private static async Task GetWriteTask(LightBuffer[] buffer, ICryptoTransform sha, bool allowSimultaneousIo, AsyncLock ioLocker, FileStream destinationStream, Action<long> progressCallback)
        {
            var writeIdx = 0;
            var run = true;
            var writeDone = 0L;
            while (run)
            {
                var lightBuffer = buffer[writeIdx];
                await lightBuffer.DataReady.WaitAsync();

                run = !lightBuffer.IsFinal;

                var hashTask = Task.Run(() =>
                {
                    if (lightBuffer.IsFinal)
                        sha.TransformFinalBlock(lightBuffer.Data, 0, lightBuffer.Length);
                    else
                        sha.TransformBlock(lightBuffer.Data, 0, lightBuffer.Length, null, 0);
                });

                if (allowSimultaneousIo)
                {
                    await destinationStream.WriteAsync(lightBuffer.Data, 0, lightBuffer.Length);
                }
                else
                {
                    using (await ioLocker.LockAsync())
                    {
                        await destinationStream.WriteAsync(lightBuffer.Data, 0, lightBuffer.Length);
                    }
                }

                await hashTask;

                writeDone += lightBuffer.Length;

                lightBuffer.WriteDone.Set();

                progressCallback?.Invoke(writeDone);

                Increment(ref writeIdx);
            }
        }

        private static void Increment(ref int idx)
        {
            idx++;
            if (idx > BuffersCount - 1)
                idx = 0;
        }
        private static async Task GetReadTask(long toRead, LightBuffer[] buffer, bool allowSimultaneousIo, Stream sourceStream, int readSize, AsyncLock locker)
        {
            if (toRead == 0)
            {
                buffer[0].IsFinal = true;
                buffer[0].DataReady.Set();
                return;
            }

            var readIdx = 0;
            while (toRead > 0)
            {
                var lightBuffer = buffer[readIdx];
                await lightBuffer.WriteDone.WaitAsync();

                if (allowSimultaneousIo)
                {
                    lightBuffer.Length = await sourceStream.ReadAsync(lightBuffer.Data, 0, readSize);
                    if (lightBuffer.Length == 0)
                    {
                        Debugger.Break();
                        throw null;
                    }
                }
                else
                {
                    using (await locker.LockAsync())
                    {
                        lightBuffer.Length = await sourceStream.ReadAsync(lightBuffer.Data, 0, readSize);
                        if (lightBuffer.Length == 0)
                        {
                            Debugger.Break();
                            throw null;
                        }
                    }
                }

                toRead -= lightBuffer.Length;

                lightBuffer.IsFinal = toRead == 0;
                lightBuffer.DataReady.Set();

                Increment(ref readIdx);
            }
        }

        private sealed class LightBuffer
        {
            public LightBuffer(int size)
            {
                Data = new byte[size];
            }

            public byte[] Data { get; }

            public int Length { get; set; }

            public AsyncAutoResetEvent DataReady { get; } = new AsyncAutoResetEvent(false);

            public AsyncAutoResetEvent WriteDone { get; } = new AsyncAutoResetEvent(true);

            public bool IsFinal { get; set; }
        }
    }
}