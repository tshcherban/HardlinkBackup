using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using HardLinkBackup;
using Renci.SshNet;

namespace Backuper
{
    public static class Program
    {
        private static int? _previousCategory;
        private static string _logFileName;

        private static bool TryGetParameters(string[] args, string name, out string[] value)
        {
            value = args.Where(x => x.StartsWith(name)).Select(x=>x.Replace(name, null)).ToArray();
            return value.Length > 0;
        }

        private static bool TryGetParameter(string[] args, string name, out string value)
        {
            value = null;
            value = args.FirstOrDefault(x => x.StartsWith(name))?.Replace(name, null);
            return !string.IsNullOrEmpty(value);
        }

        private static string[] GetParametersOrDefault(string[] args, string name)
        {
            return TryGetParameters(args, name, out var value) ? value : null;
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
            System.Diagnostics.Debugger.Launch();

            Console.OutputEncoding = System.Text.Encoding.UTF8;

            if (args == null || args.Length == 0)
            {
                Console.WriteLine("No args, nothing to do");
                return;
            }

            if (args.Length == 1 && TryGetParameter(args, "-bdf:", out var backupDefinitionFile))
                args = File.ReadAllLines(backupDefinitionFile);

            var backupParams = new BackupParams
            {
                Sources = GetParametersOrDefault(args, "-s:"),
                Target = GetParameterOrDefault(args, "-t:"),
                SshLogin = GetParameterOrDefault(args, "-sl:"),
                SshPassword = GetParameterOrDefault(args, "-sp:"),
                SshHost = GetParameterOrDefault(args, "-sh:"),
                SshRootDir = GetParameterOrDefault(args, "-sr:"),
                SshPort = TryGetParameter(args, "-spp:", out var sshPort) ? int.Parse(sshPort) : (int?) null,
                LogFile = GetParameterOrDefault(args, "-l:"),
            };

            var rootDir = string.Empty;
            var spl = backupParams.Sources.Select(x => x.Split('\\')).ToList();
            for (var i = 0; i < spl.Min(x=>x.Length); ++i)
            {
                var partsFromAllSources = spl.Select(x => x[i]).Distinct().ToList();
                if (partsFromAllSources.Count == 1)
                    rootDir += partsFromAllSources[0] + "\\";
                else
                    break;
            }

            if (string.IsNullOrEmpty(rootDir))
            {
                Console.WriteLine("Root directory can not be determined");
                return;
            }

            backupParams.RootDit = rootDir;

            await Backup(backupParams);

            Console.WriteLine("Done. Press return to exit");

            Console.ReadLine();
        }

        private static async Task Backup(BackupParams backupParams)
        {
            if (backupParams.Sources.Length == 0)
            {
                Console.WriteLine("Source folder is not specified");
                return;
            }

            /*if (!Directory.Exists(backupParams.Source))
            {
                Console.WriteLine("Source folder does not exist");
                return;
            }*/

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
                using (var vssHelper = new VssHelper(new DirectoryInfo(backupParams.RootDit).Root.Name))
                {
                    Console.WriteLine("Creating VSS snapshot...");

                    var actualRoot = vssHelper.CreateSnapshot()
                        ? vssHelper.GetSnapshotFilePath(backupParams.RootDit)
                        : backupParams.RootDit;

                    string[] TargetFilesEnumerator()
                    {
                        var cmd = sshClient.RunCommand($"find \"{backupParams.SshRootDir}\" -type f");
                        var result = cmd.Result;
                        var es = cmd.ExitStatus;

                        if (es != 0 || string.IsNullOrEmpty(result)) return new string[0];

                        var files = result.Split(new[] {"\r", "\n", "\r\n"}, StringSplitOptions.RemoveEmptyEntries)
                            .Where(x => !x.EndsWith(".bkp/info.json"))
                            .Select(x => x.Replace(backupParams.SshRootDir, backupParams.Target))
                            .ToArray();

                        return files;
                    }

                    var engine = new HardLinkBackupEngine(actualRoot, backupParams.Sources, backupParams.Target, true, helper, TargetFilesEnumerator);
                    engine.Log += WriteLog;
                    engine.LogExt += WriteLogExt;
                    await engine.DoBackup();
                }

                sw.Stop();

                Console.WriteLine($"Done in {sw.Elapsed:hh\\:mm\\:ss}");
            }
            catch (Exception e)
            {
                sw.Stop();

                Console.WriteLine($"Failed (operation lasted {sw.Elapsed:hh\\:mm\\:ss})");
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