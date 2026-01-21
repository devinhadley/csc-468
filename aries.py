from collections import defaultdict
import json

# 1.) Analysis
# - We first reconstruct the transaction and dirty page table to determine which transactions were commited and not commited.
# - Commited transactions are winners, and non-commited transactions are losers.
# 2.) Redo
# - At the time of the crash the memory and disk were in unknown state.
# - To remedy this we redo all tranactions to put the disk into a form we definitively understand.
# - Redo replays all necessary updates from the WAL to ensure the disk reflects all logged history, producing a definitive, known state.
# 3.) Undo
# - Then we undo the uncommited transactions.
# - Undo rolls back only the uncommitted (loser) transactions by applying before-images, restoring the database to a state that reflects only committed transactions.

# Analysis: Scan the WAL forward (from the last checkpoint) to reconstruct the
# Transaction Table and Dirty Page Table, identifying committed (winner) and
# uncommitted (loser) transactions.
#
# Redo: Because the disk state at crash is unknown (STEAL/NO-FORCE), scan
# forward from the earliest recLSN and reapply logged updates as needed to
# put the database into a known, consistent state.
#
# Undo: Scan the WAL backward and undo the effects of uncommitted (loser)
# transactions.

# High level understanding: The DBMS takes charge of managing memory in entirety.
# This means it may flush to disk before a commit (steal) and may not flush to disk
# following a commmit (no-force). Thus, if there is a failure we need to restore
# the disk to a known state, and then undo the uncommited changes

# lastLSN (last LSN for transaction) = the the most recent log record wirrtten by a given transaction.
# This is the point which undo will rollback from for loser transactions.

# recLSN (recovery LSN) = the earliest log record whose effects on the page are not guarenteed to be on disk.
# It is where redo should begin from.

# Why redo and undo?
# Because the state of the disk is unknown at crash time (steal, no force).
# By redoing everything, we put the db in a state where we can revert
# Redo → solves NO-FORCE (winner data might be missing from disk)
# Undo → solves STEAL (loser data might be present on disk)
# We redo losers because we need to walk back the existing changes.

DEFAULT_WAL_FILE_PATH = "./files/wal.jsonl"


def redo(dirty_page_table: dict, wal_path=DEFAULT_WAL_FILE_PATH) -> None:
    # Redo all updates starting from the recovery lsn.
    # We do this in order to ensure that winners are written to disk (fix no-force), and
    # to put losers in a state that we can undo from (fix steal).

    min_recovery_lsn = min(dirty_page_table.values())

    lines = None
    with open(wal_path) as f:  # NOTE: loads entire file into memory. Can be bad...
        lines = f.readlines()

    # Find the index of start WAL.

    start_index = 0
    for index, line in enumerate(lines):
        wal_entry = json.loads(line)
        if wal_entry["LSN"] == min_recovery_lsn:
            start_index = index

    # Redo every WAL update from here!
    for i in range(start_index, len(lines), 1):
        line = lines[i]
        wal_entry = json.loads(line)

        if wal_entry["type"] == "UPDATE" and (
            True
        ):  # TODO: Ensure WAL LSN is greater than the current page lsn.
            pass

        pass


def undo(transaction_table: dict) -> None:
    # Perform a single backward scan of the log to simultaneously
    # undo all operations belonging to uncommitted "loser" transactions in reverse chronological order.
    return


def _print_analysis_report(transaction_table, dirty_page_table):
    winners = [
        tx for tx, info in transaction_table.items() if info["status"] == "COMMITTED"
    ]
    losers = [
        tx for tx, info in transaction_table.items() if info["status"] != "COMMITTED"
    ]

    print("Analysis Report:")

    print("\tWinner Transaction IDs:")
    if winners:
        for tx in winners:
            print(f"\t\t{tx}")
    else:
        print("\t\tNo winner transactions.")

    print("\tLoser Transaction IDs:")
    if losers:
        for tx in losers:
            print(f"\t\t{tx}")
    else:
        print("\t\tNo loser transactions.")

    print("\tTransaction Table After Analysis:")
    print(f"\t\tTX, STATUS, lastLSN")
    for tx, info in transaction_table.items():
        print(f"\t\t{tx}, {info['status']}, {info['lastLSN']}")

    print("\tDirty Page Table After Analysis:")
    print(f"\t\tPAGE, recLSN")
    for page, rec_lsn in dirty_page_table.items():
        print(f"\t\t{page}, {rec_lsn}")


def analysis(wal_path=DEFAULT_WAL_FILE_PATH) -> tuple[dict, dict]:
    # TODO: Fill me in with what I do!
    """I return the transaction table then the dirty page table."""
    dirty_page_table = {}

    # Page mumber -> recLSN (the earliest LSN where that page was modified since it became dirty).

    transaction_table = {}

    # Goals:
    # - Construct dirty page table.
    # - Construct transaction table.

    # 1.) Scan from the large lsn to low lsn and determine the latest checkpoint.
    # 2.) Initialize the tables from the checkpoint.
    # 3.) Then scan from small lsn to large lsn to update the tables to latest state.

    # 1.) Find the index of the latest checkpoint (if any).
    lines = None
    with open(wal_path) as f:  # NOTE: loads entire file into memory. Can be bad...
        lines = f.readlines()

    checkpoint_index = None
    for i in range(len(lines) - 1, -1, -1):
        line = lines[i]

        wal_entry = json.loads(line)

        match wal_entry:
            case {"type": "CHECKPOINT", "DPT": dpt_snapshot, "TT": tt_snapshot}:
                dirty_page_table = dpt_snapshot
                transaction_table = tt_snapshot
                checkpoint_index = i
                break
            case _:
                continue

    # 2.) Now process all logs after the latest checkpoint.
    # to ensure we have the most up to date tables...

    # There was no checkpoint, we should start from 0 and include log at 0.
    if checkpoint_index is None:
        checkpoint_index = -1

    for i in range(checkpoint_index + 1, len(lines), 1):
        line = lines[i]
        wal_entry = json.loads(line)

        match wal_entry:
            case {"LSN": lsn, "type": "BEGIN", "tx": tx}:
                # Add this transaction to the transaction table...
                transaction_table[tx] = {
                    "status": "RUNNING",
                    "lastLSN": lsn,
                }
            case {
                "LSN": lsn,
                "type": "UPDATE",
                "tx": tx,
                "page": page,
            }:
                # Update the last lsn for this transaction, and update the pages
                # dirty page table entry for redo later if earliest update.
                transaction_table[tx]["lastLSN"] = lsn
                # Add this page to the dirty page table if not already in there.
                if page not in dirty_page_table:
                    dirty_page_table[page] = lsn

            case {"LSN": lsn, "type": "COMMIT", "tx": tx}:
                transaction_table[tx]["lastLSN"] = lsn
                # Will consider this transaction a winnner later doing undo...
                transaction_table[tx]["status"] = "COMMITTED"

            case {"LSN": lsn, "type": "ABORT", "tx": tx}:
                transaction_table[tx]["lastLSN"] = lsn
                # Add this transaction to the transaction table...
                transaction_table[tx]["status"] = "ABORTED"
            case {"LSN": _, "type": "END", "tx": tx}:
                # No longer need to manage this transaction...
                # A winner of sorts...
                transaction_table.pop(tx)

    return transaction_table, dirty_page_table


# Expected Output
#
#    [x] Print winner and loser transactions.
#
#    [x] Print TT and DPT after Analysis.
#
#    [ ] Print which updates are redone (by LSN).
#
#    [ ] Print which updates are undone (by LSN).
#
#    [ ] Output final disk_pages_after.json representing disk state after recovery (value + pageLSN per page).


def main():
    transaction_table, dirty_page_table = analysis()
    _print_analysis_report(transaction_table, dirty_page_table)
    redo(dirty_page_table)
    undo(transaction_table)
    return


if __name__ == "__main__":
    main()
