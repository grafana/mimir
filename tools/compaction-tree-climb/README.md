# compaction-tree-climb

When a split-and-merge merge job fails because its output block would be too large (for example, its index would exceed 64GiB), the compactor marks the job's blocks for no compaction. Nothing above those blocks in their shard tree compacts again until someone intervenes.

This tool gets such a tree compacting again in steps. You split each failed job into smaller groups of blocks. Then, one group per tree at a time, the tool lets the group compact on its own. Each group compacts into a smaller block, and that block stays out of further compaction.

The tool never changes the bucket. It reads the tenant's bucket index and lists the tenant's no-compact marks, and writes files for [`mimirtool blocks mark`](../../cmd/mimirtool) to apply.

The bucket index can be out of date with the live state of the bucket, and the tool doesn't see blocks added since the index was last updated.

## How it works

A tree is the set of blocks that compaction would eventually merge together: blocks with the same external labels and shard ID within the same largest block range.

Each run:

1. Skips trees that have a group from an earlier run whose blocks are unmarked but not yet compacted.
2. Picks the first remaining group of each other tree.
3. Finds the tree's other unmarked blocks. These must be marked, so the group's compacted block isn't merged with them into another block that's too large.
4. Runs the compaction planner against the state after the change. Each chosen group must compact as exactly one job, and its compacted block must not be planned for any further compaction.
5. Writes the blocks to mark and the blocks to unmark, and records the chosen groups in the state file.

## Usage

Bucket flags are the same as `mimirtool blocks mark`'s, for example `-backend gcs -gcs.bucket-name <bucket>`. The examples below write them as `<bucket flags>`. The tenant's `-shard-count`, `-ooo-shard-count`, `-split-groups` and `-block-ranges` must match its compactor configuration, so the tool plans the same jobs the compactor does.

### 1. Write the groups file

The groups file (`-groups-file`) is a JSON object mapping each group to its block IDs. A group's name can be anything, for example the key of the failed compaction job, as the compactor logs it, followed by `-` and a suffix. Groups are chosen in name order:

```json
{
  "<job key>-1": ["01K...", "01K...", "01K..."],
  "<job key>-2": ["01K...", "01K...", "01K..."]
}
```

A group must have at least 2 blocks, a block can be in only one group, and all of a group's blocks must be in the same tree. Keep the file unchanged for the whole climb.

### 2. Choose groups

```sh
go run ./tools/compaction-tree-climb <bucket flags> \
  -tenant <tenant> -shard-count <shards> \
  -groups-file groups.json
```

To only choose groups within a time range, pass `-min-time`, `-max-time` or both, for example `-min-time 2026-10-24T00:00:00Z -max-time 2026-10-25T00:00:00Z`. A group is skipped if any of its blocks starts before `-min-time` or ends after `-max-time`.

The tool prints the index's update time and the available groups, then asks how many of the available groups to choose. Each group is one compaction job, so this bounds the load put on the compactors. It then writes:

- `mark` (`-mark-file`): blocks to mark for no compaction. It isn't written if there are none.
- `unmark` (`-unmark-file`): blocks of the chosen groups to remove no-compact marks from.
- `chosen-groups` (`-state-file`): the chosen groups are appended to it, so later runs skip them.

The mark and unmark files must not already exist. Move or delete the files from the previous run first.

### 3. Apply the files

Run the commands the tool printed, marking before unmarking:

```sh
mimirtool blocks mark <bucket flags> --mark-type no-compact --details compaction-tree-climb --blocks-file mark
mimirtool blocks mark <bucket flags> --mark-type no-compact --remove --blocks-file unmark
```

Each line of the files is `<tenant>/<block ID>`, so don't pass `--tenant`. Add `--dry-run` first to review the changes.

The chosen groups are added to the state file when the files are written, not when they're applied. If you don't apply them, remove the lines that run appended, which are the last ones in the state file. The tool prints how many lines to remove and the groups they name.

### 4. Repeat

Once the compactors have compacted the groups and the bucket index has been updated, run step 2 again. Trees whose groups haven't compacted yet are skipped, so running early chooses nothing new for them. The climb is done when no groups are chosen.

If a group's compaction fails and its blocks are marked again, its tree moves on to its next group.
