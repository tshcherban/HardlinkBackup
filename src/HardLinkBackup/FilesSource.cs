namespace HardLinkBackup
{
    public class FilesSource
    {
        public FilesSource(string fullPath, string alias)
        {
            FullPath = fullPath;
            Alias = alias;
        }

        public string FullPath { get; }

        public string Alias { get; }
    }
}