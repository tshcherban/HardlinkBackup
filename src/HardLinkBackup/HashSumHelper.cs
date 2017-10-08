using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace HardLinkBackup
{
    public static class HashSumHelper
    {
        public static byte[] ComputeSha256Unbuffered(string filePath)
        {
            const FileOptions fileFlagNoBuffering = (FileOptions)0x20000000;
            const FileOptions fileOptions = fileFlagNoBuffering | FileOptions.SequentialScan;

            const int chunkSize = 32 * 1024 * 1024;

            var readBufferSize = chunkSize;
            readBufferSize += ((readBufferSize + 1023) & ~1023) - readBufferSize;

            var dataToProcess = new ConcurrentQueue<Buffer>();
            var reusableData = new ConcurrentQueue<Buffer>();

            using (HashAlgorithm sha = SHA256.Create())
            {
                var allowExit = false;
                var e = new ManualResetEvent(false);

                int rcount = 1, ccount = 1;

                var task = Task.Factory.StartNew(() =>
                {
                    while (true)
                    {
                        if (dataToProcess.TryDequeue(out var buffer))
                        {
                            Debug.WriteLine($"Computing... {ccount++}");
                            if (!buffer.IsLast)
                                sha.TransformBlock(buffer.Data, 0, buffer.Length, buffer.Data, 0);
                            else
                                sha.TransformFinalBlock(buffer.Data, 0, buffer.Length);

                            buffer.IsLast = false;
                            buffer.Length = 0;
                            reusableData.Enqueue(buffer);
                        }
                        else if (allowExit)
                            return;
                        e.Set();
                    }
                }, TaskCreationOptions.LongRunning);
            
                using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fileOptions))
                {
                    var length = stream.Length;
                    var toRead = length;

                    var readSize = Convert.ToInt32(Math.Min(chunkSize, length));

                    while (toRead > 0)
                    {
                        if (dataToProcess.Count > 0)
                        {
                            Debug.WriteLine("Waiting...");
                            e.WaitOne();
                            e.Reset();
                        }

                        if (!reusableData.TryDequeue(out var buffer))
                        {
                            Debug.WriteLine("Allocating...");
                            var array = new byte[readSize];
                            buffer = new Buffer {Data = array};
                        }
                    
                        Debug.WriteLine($"Reading...{rcount++}");
                        buffer.Length = stream.Read(buffer.Data, 0, readSize);
                        if (buffer.Length == 0)
                            throw new EndOfStreamException("Read beyond end of file EOF");

                        toRead -= buffer.Length;

                        buffer.IsLast = toRead == 0;

                        dataToProcess.Enqueue(buffer);
                    }

                    allowExit = true;

                    task.Wait();

                    while (reusableData.TryDequeue(out var _)) ;
                    while (dataToProcess.TryDequeue(out var _)) ;

                    reusableData = null;
                    dataToProcess = null;

                    return sha.Hash;
                }
            }
        }

        private class Buffer
        {
            public byte[] Data;

            public int Length;

            public bool IsLast { get; set; }
        }
    }
}