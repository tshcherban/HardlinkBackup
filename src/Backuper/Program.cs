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

        private static bool TryGetParameter(string[] args, string name, out string value)
        {
            value = null;
            value = args.FirstOrDefault(x => x.StartsWith(name))?.Replace(name, null);
            return !string.IsNullOrEmpty(value);
        }

        private static string GetParameter(string[] args, string name)
        {
            if (!TryGetParameter(args, name, out var value))
                throw new Exception("Failed to get arg " + name);

            return value;
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

            var p = new BackupParams
            {
                Source = GetParameterOrDefault(args, "-s:"),
                Target = GetParameterOrDefault(args, "-t:"),
                SshLogin = GetParameter(args, "-sl:"),
                SshPassword = GetParameter(args, "-sp:"),
                SshHost = GetParameter(args, "-sh:"),
                SshRootDir = GetParameter(args, "-sr:"),
                SshPort = int.Parse(GetParameter(args, "-spp:")),
                LogFile = GetParameter(args, "-l:"),
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

                await BackupHardLinks(backupParams.Source, backupParams.Target, hardLinkHelper);
            }
        }

        private static async Task BackupHardLinks(string source, string target, IHardLinkHelper helper)
        {
            Console.CursorVisible = false;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                using (var vssHelper = new VssHelper(new DirectoryInfo(source).Root.Name))
                {
                    Console.WriteLine("Creating VSS snapshot...");

                    var actualSource = vssHelper.CreateSnapshot()
                        ? vssHelper.GetSnapshotFilePath(source)
                        : source;

                    var engine = new HardLinkBackupEngine(actualSource, target, true, helper);
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