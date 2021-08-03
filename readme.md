Provides incremental backups using ntfs/linux hardlinks.
If target is a network share with linux - hardlink are maintained over ssh by ln command.
Small files are transferred as a compressed tar to reduce network overhead.
