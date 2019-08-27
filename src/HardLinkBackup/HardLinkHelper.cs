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
        private readonly ConnectionInfo _connectionInfo;

        public NetShareSshHardLinkHelper(string rootDirToReplace, string realRootDir, string host, string user, string password)
        {
            _rootDirToReplace = rootDirToReplace;
            _realRootDir = realRootDir;
            _connectionInfo = new ConnectionInfo(host, user, new PasswordAuthenticationMethod(user, password));
        }

        public bool CreateHardLink(string source, string target)
        {
            source = source.Replace(_rootDirToReplace, _realRootDir);
            target = target.Replace(_rootDirToReplace, _realRootDir);

            using (var client = new SshClient(_connectionInfo))
            {
                client.Connect();
                var cmd = client.RunCommand($"ln {source} {target}");
                return string.IsNullOrEmpty(cmd.Result);
            }
        }
    }
}