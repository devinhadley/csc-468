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


def redo():
    pass


def undo():
    return


def analysis():
    return


def main():
    return


if __name__ == "__main__":
    main()
