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

        public void AddHardLinkToQueue(string source, string target)
        {
            source = source.Replace(_rootDirToReplace, _realRootDir).Replace('\\', '/');
            target = target.Replace(_rootDirToReplace, _realRootDir).Replace('\\', '/');

            var cmd = $"ln \"{source}\" \"{target}\"";

            _linkQueue.Add(cmd);
        }

        public void UnpackTar(string tarFilePath)
        {
            var unixTarFilePath = tarFilePath.Replace(_rootDirToReplace, _realRootDir).Replace('\\', '/');
            var targetDir = Path.GetDirectoryName(unixTarFilePath).Replace(".bkp", null).Replace('\\', '/');

            var cmd = $"tar -xzf {unixTarFilePath} -C {targetDir}";
            var result = _client.RunCommand(cmd);
            if (result.ExitStatus != 0 || !string.IsNullOrEmpty(result.Error))
                throw new Exception(result.Error);

            File.Delete(tarFilePath);
        }

        public void CreateHardLinks()
        {
            const int commandSizeLimit = 20000;

            var cmdBuilder = new StringBuilder(commandSizeLimit);

            while (_linkQueue.Count > 0)
            {
                cmdBuilder.Clear();

                while (_linkQueue.Count > 0 && cmdBuilder.Length < Math.Max(1, commandSizeLimit - _linkQueue[_linkQueue.Count - 1].Length * 2))
                {
                    var f = _linkQueue[_linkQueue.Count - 1];
                    _linkQueue.RemoveAt(_linkQueue.Count - 1);
                    cmdBuilder.Append($"{f}\n");
                }

                var commandText = cmdBuilder.ToString();
                var cmd = _client.RunCommand(commandText);
                if (cmd.ExitStatus != 0 || !string.IsNullOrEmpty(cmd.Error))
                {
                    var msg = string.IsNullOrEmpty(cmd.Error)
                        ? "Ssh command failed with exit code " + cmd.ExitStatus
                        : cmd.Error;

                    throw new Exception(msg);
                }
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