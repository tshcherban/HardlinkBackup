using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace HardLinkBackup
{
    public class BackupInfo
    {
        private const string BackupInfoDir = ".bkp";
        private const string BackupInfoFile = "info.json";

        public List<BackupFileInfo> Objects { get; set; }

        public DateTime DateTime { get; set; }

        [JsonIgnore]
        public string AbsolutePath { get; set; }

        public void CheckIntegrity()
        {
            if (!Directory.Exists(AbsolutePath))
                throw new InvalidOperationException("Backup directory not found");

            foreach (var f in Objects)
            {
                var fileName = AbsolutePath + f.Path;
                if (!File.Exists(fileName))
                    throw new InvalidOperationException("File was removed");

                var fi = new FileInfoEx(fileName);
                if (f.Hash != fi.FastHashStr)
                    throw new InvalidOperationException("File corrupt");
            }
        }

        public static IEnumerable<BackupInfo> DiscoverBackups(string path)
        {
            var dirs = Directory.EnumerateDirectories(path);
            foreach (var dir in dirs)
            {
                BackupInfo info;
                try
                {
                    var bkpInfoDir = Directory.EnumerateDirectories(dir)
                        .FirstOrDefault(d => new DirectoryInfo(d).Name == BackupInfoDir);
                    if (string.IsNullOrEmpty(bkpInfoDir))
                        continue;

                    info = JsonHelpers.ReadFromFile<BackupInfo>(Path.Combine(bkpInfoDir, BackupInfoFile));
                    info.AbsolutePath = dir;
                }
                catch (Exception)
                {
                    continue;
                }
                yield return info;
            }
        }

        public void WriteToDisk()
        {
            if (!Directory.Exists(AbsolutePath))
                Directory.CreateDirectory(AbsolutePath);

            var bkpInfoDir = Path.Combine(AbsolutePath, BackupInfoDir);
            Directory.CreateDirectory(bkpInfoDir);

            var bkpInfoFile = Path.Combine(bkpInfoDir, BackupInfoFile);
            JsonHelpers.WriteToFile(bkpInfoFile, this);
        }
    }
}