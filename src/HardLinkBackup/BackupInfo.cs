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
        private const string BackupInfoFile = "info.txt";
        private const string DateSerializationFormat = "yyyy.MM.dd HH:mm:ss";

        private readonly Dictionary<long, Dictionary<string, BackupFileInfo>> _filesLookup;
        private readonly List<BackupFileInfo> _files = new List<BackupFileInfo>();

        private string _serviceDir;

        public IReadOnlyList<BackupFileInfo> Files
        {
            get { return _files; }
        }

        public DateTime DateTime { get; set; }

        public string AbsolutePath { get; }

        public bool AttributesAvailable { get; set; }

        public BackupInfo(string absolutePath)
        {
            AbsolutePath = absolutePath;
            _filesLookup = new Dictionary<long, Dictionary<string, BackupFileInfo>>();
        }

        public void CheckIntegrity()
        {
            if (!Directory.Exists(AbsolutePath))
                throw new InvalidOperationException("Backup directory not found");

            foreach (var f in _files)
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
                var info = new BackupInfo(dir);
                try
                {
                    var bkpInfoDir = Directory.EnumerateDirectories(dir).FirstOrDefault(d => new DirectoryInfo(d).Name == BackupInfoDir);
                    if (string.IsNullOrEmpty(bkpInfoDir))
                        continue;

                    var bkpInfoFile = Path.Combine(bkpInfoDir, BackupInfoFile);
                    if (!File.Exists(bkpInfoFile))
                        continue;

                    using (var file = File.OpenText(bkpInfoFile))
                    {
                        var headerRaw = file.ReadLine();
                        if (string.IsNullOrEmpty(headerRaw))
                            continue;

                        var headerParts = headerRaw.Contains("|") ? headerRaw.Split('|') : new[] {headerRaw};

                        if (headerParts.Length == 0)
                            continue;

                        if (DateTime.TryParseExact(headerParts[0], DateSerializationFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                            info.DateTime = dt;
                        else
                            continue;

                        if (headerParts.Length > 2)
                            continue;

                        info.AttributesAvailable = headerParts.Length == 2 && headerParts[1] == "CM";

                        while (!file.EndOfStream)
                        {
                            var line = file.ReadLine();
                            var parts = line?.Split('|');
                            if (parts == null)
                                throw new Exception("Failed to parse backup files list. Empty file line");

                            if (parts.Length != 3 && parts.Length != 5)
                                throw new Exception($"Failed to parse backup files list. Unknown file line '{line}'");

                            var fi = new BackupFileInfo
                            {
                                Path = parts[0],
                                Hash = parts[1],
                                Length = long.Parse(parts[2]),
                            };

                            if (parts.Length == 5)
                            {
                                fi.Created = DateTime.ParseExact(parts[3], DateSerializationFormat, CultureInfo.InvariantCulture, DateTimeStyles.None);
                                fi.Modified = DateTime.ParseExact(parts[4], DateSerializationFormat, CultureInfo.InvariantCulture, DateTimeStyles.None);
                            }

                            info._files.Add(fi);
                        }
                    }
                }
                catch (Exception)
                {
                    continue;
                }

                yield return info;
            }
        }

        public string CreateFolders()
        {
            if (!Directory.Exists(AbsolutePath))
                Directory.CreateDirectory(AbsolutePath);

            _serviceDir = Path.Combine(AbsolutePath, BackupInfoDir);
            Directory.CreateDirectory(_serviceDir);

            return _serviceDir;
        }

        public void WriteToDisk()
        {
            Directory.CreateDirectory(_serviceDir);

            var bkpInfoFile = Path.Combine(_serviceDir, BackupInfoFile);

            using (var file = File.CreateText(bkpInfoFile))
            {
                file.WriteLine($"{DateTime.ToString(DateSerializationFormat)}|CM");
                foreach (var o in _files)
                {
                    file.Write(o.Path);
                    file.Write('|');
                    file.Write(o.Hash);
                    file.Write('|');
                    file.Write(o.Length);
                    file.Write('|');
                    file.Write(o.Created.ToString(DateSerializationFormat));
                    file.Write('|');
                    file.WriteLine(o.Modified.ToString(DateSerializationFormat));
                }
            }
        }

        public BackupFileInfo FindFile(FileInfoEx fInfoEx)
        {
            if (_filesLookup.TryGetValue(fInfoEx.FileInfo.Length, out var byHash))
                return byHash.TryGetValue(fInfoEx.FastHashStr, out var file) ? file : null;

            return null;
        }

        public void AddFile(BackupFileInfo fileInfo)
        {
            _files.Add(fileInfo);
            if (fileInfo.IsLink)
                return;

            if (!_filesLookup.TryGetValue(fileInfo.Length, out var files))
            {
                files = new Dictionary<string, BackupFileInfo>();
                _filesLookup[fileInfo.Length] = files;
            }

            if(!files.ContainsKey(fileInfo.Hash))
                files[fileInfo.Hash] = fileInfo;
        }

        public void CreateIncompleteAttribute()
        {
            var incompleteAttributeFile = Path.Combine(_serviceDir, "incomplete_backup.txt");
            File.WriteAllText(incompleteAttributeFile, "This backup is in progress or has been interrupted");
        }

        public void DeleteIncompleteAttribute()
        {
            var incompleteAttributeFile = Path.Combine(_serviceDir, "incomplete_backup.txt");
            File.Delete(incompleteAttributeFile);
        }
    }
}