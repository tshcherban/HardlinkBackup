using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using HardLinkBackup;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using ICSharpCode.SharpZipLib.Zip.Compression;
using Renci.SshNet;

namespace Backuper
{
    public static class Program
    {
        private static int? _previousCategory;
        private static string _logFileName;

        private static bool TryGetParameter(string[] args, string name, out string value)
        {
            value = null;
            value = args.FirstOrDefault(x => x.StartsWith(name))?.Replace(name, null);
            return !string.IsNullOrEmpty(value);
        }

        private static string GetParameterOrDefault(string[] args, string name)
        {
            return TryGetParameter(args, name, out var value) ? value : null;
        }

        // -s:      <source>
        // -t:      <target>
        // -sl:     <ssh login>
        // -sp:     <ssh password>
        // -sr:     <ssh root dir>
        // -sh:     <ssh host>
        // -spp:    <ssh port>
        // -l:      <log>
        // -bdf:    <backup definition file>
        public static async Task Main(string[] args)
        {
            using (new NetworkConnection($@"\\192.168.56.102", new NetworkCredential("synotest", "123456")))
            {
                var t = @"\\192.168.56.102\share\small-files-test";
                var s = @"C:\shcherban\gitlab\vsonline";

                if (Directory.Exists(t))
                    Directory.Delete(t, true);
                Directory.CreateDirectory(t);

                var filesToCopy = Directory.EnumerateFiles(s, "*", SearchOption.AllDirectories)
                    .Select(x => new
                    {
                        src = x,
                        tgt = Path.Combine(t, x.Replace(s, null).Replace('\\', '_')),
                        tgtRel = x.Replace(s, null)/*.Replace('\\', '_')*/,
                    })
                    .ToList();

                var sw = System.Diagnostics.Stopwatch.StartNew();
                /*foreach (var f in filesToCopy)
                {
                    File.Copy(f.src, f.tgt);
                }
                sw.Stop();*/

                Console.WriteLine($"{sw.Elapsed:g}");

                Directory.Delete(t, true);
                Directory.CreateDirectory(t);
                using (var ssh = new SshClient("192.168.56.102", 22, "synotest", "123456"))
                {
                    ssh.Connect();
                    sw = System.Diagnostics.Stopwatch.StartNew();

                    /*using (var zf = File.Create(Path.Combine(t, "zip.zip")))
                    {
                        using (var zip = new ZipArchive(zf, ZipArchiveMode.Create))
                        {
                            foreach (var f in filesToCopy)
                            {
                                var entry = zip.CreateEntry(f.tgtRel, CompressionLevel.NoCompression);
                                using (var entryStream = entry.Open())
                                {
                                    using (var srcFile = File.OpenRead(f.src))
                                    {
                                        srcFile.CopyTo(entryStream);
                                    }
                                }
                            }
                        }
                    }*/

                    var tarFile = Path.Combine(t, "zip.tar.gz");
                    using (var outStream = File.Create(tarFile))
                    using (var gzoStream = new GZipOutputStream(outStream))
                    using (var tarArchive = TarArchive.CreateOutputTarArchive(gzoStream))
                    {
                        gzoStream.SetLevel(Deflater.BEST_SPEED);

                        foreach (var f in filesToCopy)
                        {
                            tarArchive.RootPath = Path.GetDirectoryName(f.src);

                            var tarEntry = TarEntry.CreateEntryFromFile(f.src);
                            tarEntry.Name = f.tgtRel.Replace(@"\\", null).TrimStart('\\').Replace('\\', '/');

                            tarArchive.WriteEntry(tarEntry, true);
                        }
                    }

                    var result = ssh.RunCommand("tar -xzf /volume1/share/small-files-test/zip.tar.gz -C /volume1/share/small-files-test/");
                    if (result.ExitStatus != 0 || !string.IsNullOrEmpty(result.Error))
                        throw new Exception(result.Error);

                    //File.Delete(tarFile);
                    var res = result.Result;
                }
                

                sw.Stop();

                Console.WriteLine($"tar.gz {sw.Elapsed:g}");
                Console.ReadLine();
            }

            return;

            System.Diagnostics.Debugger.Launch();

            Console.OutputEncoding = System.Text.Encoding.UTF8;

            if (args == null || args.Length == 0)
            {
                Console.WriteLine("No args, nothing to do");
                return;
            }

            if (args.Length == 1 && TryGetParameter(args, "-bdf:", out var backupDefinitionFile))
                args = File.ReadAllLines(backupDefinitionFile);

            var p = new BackupParams
            {
                Source = GetParameterOrDefault(args, "-s:"),
                Target = GetParameterOrDefault(args, "-t:"),
                SshLogin = GetParameterOrDefault(args, "-sl:"),
                SshPassword = GetParameterOrDefault(args, "-sp:"),
                SshHost = GetParameterOrDefault(args, "-sh:"),
                SshRootDir = GetParameterOrDefault(args, "-sr:"),
                SshPort = TryGetParameter(args, "-spp:", out var sshPort) ? int.Parse(sshPort) : (int?) null,
                LogFile = GetParameterOrDefault(args, "-l:"),
            };

            await Backup(p);

            Console.WriteLine("Done. Press return to exit");

            Console.ReadLine();
        }

        private static async Task Backup(BackupParams backupParams)
        {
            if (string.IsNullOrEmpty(backupParams.Source))
            {
                Console.WriteLine("Source folder is not specified");
                return;
            }

            if (!Directory.Exists(backupParams.Source))
            {
                Console.WriteLine("Source folder does not exist");
                return;
            }

            if (string.IsNullOrEmpty(backupParams.Target))
            {
                Console.WriteLine("Target folder is not specified");
                return;
            }

            _logFileName = backupParams.LogFile;

            IHardLinkHelper hardLinkHelper;
            var networkConnection = Helpers.GetDummyDisposable();

            SshClient sshClient = null;
            if (!backupParams.IsSshDefined)
            {
                hardLinkHelper = new WinHardLinkHelper();
            }
            else
            {
                Console.WriteLine($"Connecting to {backupParams.SshLogin}@{backupParams.SshHost}:{backupParams.SshPort}...");

                var ci = new ConnectionInfo(backupParams.SshHost, backupParams.SshPort ?? throw new Exception("SSH port not defined"), backupParams.SshLogin, new PasswordAuthenticationMethod(backupParams.SshLogin, backupParams.SshPassword));

                sshClient = new SshClient(ci);
                sshClient.Connect();

                hardLinkHelper = new NetShareSshHardLinkHelper(backupParams.Target, backupParams.SshRootDir, sshClient);
                networkConnection = new NetworkConnection($@"\\{backupParams.SshHost}", new NetworkCredential(backupParams.SshLogin, backupParams.SshPassword));
            }

            using (networkConnection)
            using (sshClient ?? Helpers.GetDummyDisposable())
            {
                if (!Directory.Exists(backupParams.Target))
                {
                    Console.WriteLine("Target folder does not exist");
                    return;
                }

                try
                {
                    var testFile = Path.Combine(backupParams.Target, "write_access_test.txt");
                    if (File.Exists(testFile))
                        File.Delete(testFile);

                    File.WriteAllText(testFile, "Write access test file");
                    File.Delete(testFile);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to write in target directory:\r\n" + e.Message);
                    return;
                }

                await BackupHardLinks(backupParams, hardLinkHelper, sshClient);
            }
        }

        private static async Task BackupHardLinks(BackupParams backupParams, IHardLinkHelper helper, SshClient sshClient)
        {
            Console.CursorVisible = false;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                using (var vssHelper = new VssHelper(new DirectoryInfo(backupParams.Source).Root.Name))
                {
                    Console.WriteLine("Creating VSS snapshot...");

                    var actualSource = vssHelper.CreateSnapshot()
                        ? vssHelper.GetSnapshotFilePath(backupParams.Source)
                        : backupParams.Source;

                    Func<string[]> targetFilesEnumerator = () =>
                    {
                        var cmd = sshClient.RunCommand($"find \"{backupParams.SshRootDir}\" -type f");
                        var result = cmd.Result;
                        var es = cmd.ExitStatus;

                        if (es != 0 || string.IsNullOrEmpty(result))
                            return new string[0];

                        var files = result.Split(new[] {"\r", "\n", "\r\n"}, StringSplitOptions.RemoveEmptyEntries)
                            .Where(x => !x.EndsWith(".bkp/info.json"))
                            .Select(x => x.Replace(backupParams.SshRootDir, backupParams.Target))
                            .ToArray();

                        return files;

                    };

                    var engine = new HardLinkBackupEngine(actualSource, backupParams.Target, true, helper, targetFilesEnumerator);
                    engine.Log += WriteLog;
                    engine.LogExt += WriteLogExt;
                    await engine.DoBackup();
                }

                sw.Stop();

                Console.WriteLine($"Done in {sw.Elapsed:hh\\:mm\\:ss}");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                sw.Stop();
                Console.CursorVisible = true;
            }
        }

        private static void WriteLogExt(string msg)
        {
            var left = Console.CursorLeft;

            Console.Write("".PadRight(Console.BufferWidth - 1 - left));
            Console.CursorLeft = left;

            Console.Write(msg);

            Console.CursorLeft = left;
        }

        private static void WriteLog(string msg, int category)
        {
            Console.CursorLeft = 0;
            Console.Write("".PadRight(Console.BufferWidth - 1));
            Console.CursorLeft = 0;

            if (category == _previousCategory)
                Console.Write(msg);
            else
                Console.WriteLine(msg);

            if (!string.IsNullOrEmpty(_logFileName))
                File.AppendAllText(_logFileName, msg + "\r\n");

            _previousCategory = category;
        }
    }
}