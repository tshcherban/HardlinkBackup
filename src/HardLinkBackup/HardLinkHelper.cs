using System;
using System.Runtime.InteropServices;
using Renci.SshNet;

namespace HardLinkBackup
{
    public interface IHardLinkHelper
    {
        bool CreateHardLink(string source, string target);
    }

    public class WinHardLinkHelper : IHardLinkHelper
    {
        public bool CreateHardLink(string source, string target)
        {
            return CreateHardLink(target, source, IntPtr.Zero);
        }

        [DllImport("Kernel32.dll", CharSet = CharSet.Unicode)]
        private static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);
    }

    public class NetShareSshHardLinkHelper : IHardLinkHelper
    {
        private readonly string _rootDirToReplace;
        private readonly string _realRootDir;
        private readonly SshClient _client;

        public NetShareSshHardLinkHelper(string rootDirToReplace, string realRootDir, SshClient client)
        {
            _rootDirToReplace = rootDirToReplace;
            _realRootDir = realRootDir;
            _client = client;
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