# BackupHelper

A tool for simplifying the process of archiving multiple directories
onto several different drives. For each directory a checksum file 
will be created, which will be verified after the transfer.

You can stage multiple sources and add targets to them.
Once you're done you can start the transfer, which will run
all copy operations at the same time, while making sure that
all disks in a transfer aren't busy with another BackupHelper operation.

## Quick start

Add a directory as a source for copying/archiving:
```
python -m backup_helper stage ~/Documents --alias docs
Staged: /home/m/Documents
    with alias: docs
```

By default the BackupHelper state will be saved in the file
`backup_status.json` in the current working directory.
Alternatively a custom path can be used by passing
`--status-file /path/to/status.json` to __each__ command.

Add targets to that source. Either the normalized absolute path
can be used as `source` or the alias (here: _"docs"_) if present:

```
$ python -m backup_helper add-target docs /media/storage1/docs_2024 --alias storage1
Added target /media/storage1/docs_2024
    with alias: storage1
$ python -m backup_helper add-target docs /media/storage1/docs_2024 --alias storage2
Added target /media/storage1/docs_2024
    with alias: storage2
```

Now you can use the command `start` run the whole backup process
in sequence.

```
python -m backup_helper start
18:22:01 - INFO - Wrote /home/m/Documents/Documents_bh_2024-02-25T18-22-01.cshd
...
18:22:02 - INFO - 

NO MISSING FILES!

NO FAILED CHECKSUMS!

SUMMARY:
    TOTAL FILES: 3
    MATCHES: 3
    FAILED CHECKSUMS: 0
    MISSING: 0

...

18:22:02 - INFO - /home/m/Documents/Documents_bh_2024-02-25T18-22-01.cshd: No missing files and all files matching their hashes

...

18:22:02 - INFO - Successfully completed the following 5 operation(s):
Hashed '/home/m/Documents':
  Hash file: /home/m/Documents/Documents_bh_2024-02-25T18-22-01.cshd
Transfer successful:
  From: /home/m/Documents
  To: /media/storage1/docs_2024
Transfer successful:
  From: /home/m/Documents
  To: /media/storage2/docs_2024
Verified transfer '/media/storage1/docs_2024':
  Checked: 3
  CRC Errors: 0
  Missing: 0
Verified transfer '/media/storage2/docs_2024':
  Checked: 3
  CRC Errors: 0
  Missing: 0
```

Each part of the backup process can be run on its own and on a
specific source/target combination only. For more information
see the [backup process section](#backup-process).

## Backup process

The backup process, which can be run automatically using the
`start` command is split into the subprocesses:

1) Hash all source directories. The checksum file will be added to
   the directory. A log file of creating the checksum file will
   be written next to status JSON file.
2) Transfer all sources to their targets. Only one read __or__ write
   operation per disk will be allowed at the same time.
3) Verify the transfer by comparing the hashes of the generated
   checksum file with the hashes of the files on the target.
   A log of the verification process will be written to the target.

The verification process (3) will be run last if there are more
transfer operations on a disk, so:

1) More expensive write operations are performed first.
2) The transferred files are less likely to be in cache when hashing.

Each part of the backup process can be run on its own and/or on a
specific source/target combination only. Required previous steps
will be run automatically.

Using the `interactive` command it's possible to add sources/targets
while the transfer is running, otherwise all running operations would
need to be completed before executing further commands.

## Commands

See `python -m backup_helper --help`
