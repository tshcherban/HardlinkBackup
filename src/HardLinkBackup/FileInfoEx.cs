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
        private byte[] _fastHash;

        public byte[] FastHash
        {
            get { return _fastHashContainer.Value; }
            set
            {
                if (_fastHash != null)
                {
                    if (!_fastHash.SequenceEqual(value))
                        throw new Exception("Failed to assign hash: it has been already set to a different value");
                }

                _fastHash = value;
            }
        }

        public string FastHashStr => _fastHashStr ?? (_fastHashStr = string.Concat(FastHash.Select(b => $"{b:X}")));

        private byte[] HashFast()
        {
            if (_fastHash == null)
            {
                _fastHash = FileInfo.Length == 0
                    ? XxHash64Callback.EmptyHash
                    : HashHelper.HashFileAsync(FileInfo).GetAwaiter().GetResult();
            }

            return _fastHash;
        }
    }
}