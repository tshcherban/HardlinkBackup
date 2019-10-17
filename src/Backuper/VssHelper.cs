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

            if (_backup == null)
                return;

            if (_shadowCopyId != Guid.Empty)
                Helpers.ExecSafe(() => _backup.DeleteSnapshot(_shadowCopyId, true));

            Helpers.Dispose(ref _backup);
            _snapshotVolumeName = null;
            _snapshotSetId = Guid.Empty;
        }

        public string GetSnapshotFilePath(string filePath)
        {
            if (string.IsNullOrEmpty(_snapshotVolumeName) || _backup == null)
                throw new InvalidOperationException("Failed to translate local file path to snapshot, snapshot was not created or created with error");

            return filePath.Replace(_volumeName, _snapshotVolumeName);
        }

        public bool CreateSnapshot()
        {
            try
            {
                var impl = VssUtils.LoadImplementation();
                if (impl == null)
                    return false;

                _backup = impl.CreateVssBackupComponents();
                _backup.InitializeForBackup(null);

                if (!_backup.IsVolumeSupported(_volumeName))
                    return false;

                _backup.GatherWriterMetadata();

                _backup.SetContext(VssVolumeSnapshotAttributes.Persistent | VssVolumeSnapshotAttributes.NoAutoRelease);
                _backup.SetBackupState(false, true, VssBackupType.Full, false);

                _snapshotSetId = _backup.StartSnapshotSet();
                _shadowCopyId = _backup.AddToSnapshotSet(_volumeName, Guid.Empty);

                _backup.PrepareForBackup();
                _backup.DoSnapshotSet();

                _snapshotVolumeName = _backup.QuerySnapshots().First(x => x.SnapshotSetId == _snapshotSetId && x.SnapshotId == _shadowCopyId).OriginalVolumeName;

                return true;
            }
            catch
            {
                Helpers.Dispose(ref _backup);
                return false;
            }
        }
    }

    public static class Helpers
    {
        public static void ExecSafe(Action act)
        {
            try
            {
                act();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        public static void Dispose<T>(ref T disposable)
            where T : class, IDisposable
        {
            try
            {
                disposable?.Dispose();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                disposable = null;
            }
        }

        public static IDisposable GetDummyDisposable()
        {
            return new DummyDisposable();
        }

        private class DummyDisposable : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}