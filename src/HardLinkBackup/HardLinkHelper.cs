using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Renci.SshNet;

namespace HardLinkBackup
{
    public interface IHardLinkHelper
    {
        bool CreateHardLink(string source, string target);
        void CreateHardLinks();
        void AddHardLinkToQueue(string source, string target);
        void UnpackTar(string tarFilePath);
    }

    public class WinHardLinkHelper : IHardLinkHelper
    {
        public bool CreateHardLink(string source, string target)
        {
            return CreateHardLink(target, source, IntPtr.Zero);
        }

        public void CreateHardLinks()
        {
            throw new NotImplementedException();
        }

        public void AddHardLinkToQueue(string source, string target)
        {
            throw new NotImplementedException();
        }

        public void UnpackTar(string tarFilePath)
        {
            throw new NotImplementedException();
        }

        [DllImport("Kernel32.dll", CharSet = CharSet.Unicode)]
        private static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);
    }

    public class NetShareSshHardLinkHelper : IHardLinkHelper
    {
        private readonly string _rootDirToReplace;
        private readonly string _realRootDir;
        private readonly SshClient _client;
        private readonly List<string> _linkQueue;

        public NetShareSshHardLinkHelper(string rootDirToReplace, string realRootDir, SshClient client)
        {
            _rootDirToReplace = rootDirToReplace;
            _realRootDir = realRootDir;
            _client = client;
            _linkQueue = new List<string>();
        }

        public void AddHardLinkToQueue(string sourceWin, string targetWin)
        {
            var source = PathHelpers.NormalizePathUnix(sourceWin.Replace(_rootDirToReplace, _realRootDir)).Replace("$", "\\$");
            var target = PathHelpers.NormalizePathUnix(targetWin.Replace(_rootDirToReplace, _realRootDir)).Replace("$", "\\$");

            var cmd = $"ln \"{source}\" \"{target}\"";

            _linkQueue.Add(cmd);
        }

        public void UnpackTar(string tarFilePath)
        {
            var unixTarFilePath = tarFilePath.Replace(_rootDirToReplace, _realRootDir).Replace('\\', '/');
            var directoryName = Path.GetDirectoryName(unixTarFilePath);
            if (string.IsNullOrEmpty(directoryName))
                throw new Exception($"Failed to get directory name for {unixTarFilePath}");

            var targetDir = PathHelpers.NormalizePathUnix(directoryName).Replace(".bkp", null);

            var cmd = $"tar -xzf {unixTarFilePath} -C {targetDir}";
            var result = _client.RunCommand(cmd);
            if (result.ExitStatus != 0 || !string.IsNullOrEmpty(result.Error))
                throw new Exception(result.Error);

            if (!string.IsNullOrEmpty(result.Result))
                File.AppendAllText("tb1.unpacktar.txt", "unexpected result:\r\n" + result.Result);

            if (!string.IsNullOrEmpty(result.Result))
                throw new Exception($"Unexpected command result:\r\n{result.Result}");

            File.Delete(tarFilePath);
        }

        public void CreateHardLinks()
        {
            const int commandSizeLimit = 30000;

            var cmdBuilder = new StringBuilder(commandSizeLimit);

            var linkQueue = new Queue<string>(_linkQueue);

            while (linkQueue.Count > 0)
            {
                cmdBuilder.Clear();

                string linkCommand;
                do
                {
                    linkCommand = linkQueue.Dequeue();
                    cmdBuilder.Append($"{linkCommand}\n");
                } while (linkQueue.Count > 0 && cmdBuilder.Length < Math.Max(1, commandSizeLimit - linkCommand.Length * 2));

                var commandText = cmdBuilder.ToString();
                var cmd = _client.RunCommand(commandText);
                if (cmd.ExitStatus != 0 || !string.IsNullOrEmpty(cmd.Error))
                {
                    var msg = string.IsNullOrEmpty(cmd.Error)
                        ? "Ssh command failed with exit code " + cmd.ExitStatus
                        : cmd.Error;

                    throw new Exception(msg);
                }
                if (!string.IsNullOrEmpty(cmd.Result))
                    File.AppendAllText("tb1.hardlink exec result.txt", "unexpected result\r\n"+ cmd.Result);

                if (!string.IsNullOrEmpty(cmd.Result))
                    throw new Exception($"Unexpected command result:\r\n{cmd.Result}");
            }
        }

        public bool CreateHardLink(string source, string target)
        {
            source = source.Replace(_rootDirToReplace, _realRootDir).Replace('\\', '/');
            target = target.Replace(_rootDirToReplace, _realRootDir).Replace('\\', '/');

            var cmd = _client.RunCommand($"ln \"{source}\" \"{target}\"");
            return string.IsNullOrEmpty(cmd.Result) && cmd.ExitStatus == 0;
        }
    }
}