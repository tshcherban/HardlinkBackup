using System;
using System.IO;
using System.Linq;
using Backuper;

namespace HardLinkBackup
{
    public sealed class FileInfoEx
    {
        public string FileName { get; }
        public FileInfo FileInfo { get; }

        public FileInfoEx(string fileName)
        {
            FileName = fileName;
            FileInfo = new FileInfo(fileName);

            _fastHashContainer = new Lazy<byte[]>(HashFast);
        }

        private readonly Lazy<byte[]> _fastHashContainer;

        private string _fastHashStr;

        public byte[] FastHash => _fastHashContainer.Value;

        public string FastHashStr => _fastHashStr ?? (_fastHashStr = string.Concat(FastHash.Select(b => $"{b:X}")));

        private byte[] HashFast()
        {
            if (FileInfo.Length == 0)
                return XxHash64Callback.EmptyHash;

            return HashHelper.HashFileAsync(FileInfo).Result;
        }
    }
}