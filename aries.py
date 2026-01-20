from collections import defaultdict

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

_BEGIN_WAL_PROTO = {
    "LSN": None,
}


def redo():
    pass


def undo():
    return


def analysis():
    dirty_page_table = defaultdict(int)
    transaction_table = defaultdict(dict)

    # Goals:
    # - Construct dirty page table.
    # - Construct transaction table.

    # 1.) Scan from the bottom up and determine the latest checkpoint.
    # 2.) Initialize the tables from the checkpoint.
    # 3.) Then scan back down to update the tables to latest state.

    return


# Expected Output
#
#    Print winner and loser transactions.
#
#    Print TT and DPT after Analysis.
#
#    Print which updates are redone (by LSN).
#
#    Print which updates are undone (by LSN).
#
#    Output final disk_pages_after.json representing disk state after recovery (value + pageLSN per page).


def main():
    return


if __name__ == "__main__":
    main()
