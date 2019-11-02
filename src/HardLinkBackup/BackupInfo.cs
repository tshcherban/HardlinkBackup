using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace HardLinkBackup
{
    public class BackupInfo
    {
        private const string BackupInfoDir = ".bkp";
        private const string BackupInfoFile = "info.json";

        public List<BackupFileInfo> Objects { get; set; } = new List<BackupFileInfo>();

        public DateTime DateTime { get; set; }

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
                BackupInfo info = new BackupInfo();
                try
                {
                    var bkpInfoDir = Directory.EnumerateDirectories(dir).FirstOrDefault(d => new DirectoryInfo(d).Name == BackupInfoDir);
                    if (string.IsNullOrEmpty(bkpInfoDir))
                        continue;

                    using (var file = File.OpenText(Path.Combine(bkpInfoDir, BackupInfoFile)))
                    {
                        if (System.DateTime.TryParseExact(file.ReadLine(), DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                            info.DateTime = dt;
                        else
                            continue;

                        while (!file.EndOfStream)
                        {
                            var parts = file.ReadLine()?.Split('|');
                            if (parts == null || parts.Length != 3)
                                throw null;

                            var fi = new BackupFileInfo
                            {
                                Path = parts[0],
                                Hash = parts[1],
                                Length = long.Parse(parts[2]),
                            };
                            info.Objects.Add(fi);
                        }
                    }

                    info.AbsolutePath = dir;
                }
                catch (Exception)
                {
                    continue;
                }
                yield return info;
            }
        }

        const string DateFormat = "yyyy-MM-dd_HH:mm:ss";

        public string CreateFolders()
        {
            if (!Directory.Exists(AbsolutePath))
                Directory.CreateDirectory(AbsolutePath);

            var bkpInfoDir = Path.Combine(AbsolutePath, BackupInfoDir);
            Directory.CreateDirectory(bkpInfoDir);

            return bkpInfoDir;
        }

        public void WriteToDisk()
        {
            if (!Directory.Exists(AbsolutePath))
                Directory.CreateDirectory(AbsolutePath);

            var bkpInfoDir = Path.Combine(AbsolutePath, BackupInfoDir);
            Directory.CreateDirectory(bkpInfoDir);

            var bkpInfoFile = Path.Combine(bkpInfoDir, BackupInfoFile);

            using (var file = File.CreateText(bkpInfoFile))
            {
                file.WriteLine(DateTime.ToString(DateFormat));
                foreach (var o in Objects)
                {
                    file.Write(o.Path);
                    file.Write('|');
                    file.Write(o.Hash);
                    file.Write('|');
                    file.WriteLine(o.Length);
                }
            }
        }
    }
}