using System;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using Alphaleonis.Win32.Vss;

namespace Backuper
{
    public class VssHelper : CriticalFinalizerObject, IDisposable
    {
        private readonly string _volumeName;

        private bool _disposed;
        private string _snapshotVolumeName;
        private IVssBackupComponents _backup;
        private Guid _snapshotSetId;
        private Guid _shadowCopyId;

        public VssHelper(string volumeName)
        {
            _volumeName = volumeName;
            CreateSnapshot();
        }

        ~VssHelper()
        {
            Dispose();
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            GC.SuppressFinalize(this);

            _backup.DeleteSnapshot(_shadowCopyId, true);

            _backup.Dispose();
        }

        public string GetSnapshotFilePath(string filePath)
        {
            return filePath.Replace(_volumeName, _snapshotVolumeName);
        }

        private void CreateSnapshot()
        {
            var impl = VssUtils.LoadImplementation();
            _backup = impl.CreateVssBackupComponents();
            _backup.InitializeForBackup(null);
            _backup.GatherWriterMetadata();

            _backup.SetContext(VssVolumeSnapshotAttributes.Persistent | VssVolumeSnapshotAttributes.NoAutoRelease);
            _backup.SetBackupState(false, true, VssBackupType.Full, false);

            _snapshotSetId = _backup.StartSnapshotSet();
            _shadowCopyId = _backup.AddToSnapshotSet(_volumeName, Guid.Empty);

            _backup.PrepareForBackup();
            _backup.DoSnapshotSet();

            _snapshotVolumeName = _backup.QuerySnapshots().First(x => x.SnapshotSetId == _snapshotSetId && x.SnapshotId == _shadowCopyId).OriginalVolumeName;
        }
    }
}