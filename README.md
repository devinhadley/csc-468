
**Aeries Simulation Description**

The program will read disk pages and wal from ./files.  

It will then run analysis and print a report to stdout.

Then it will perform necessary redos and print the redone LSNs to stdout.

Then it will perform necessary undos and print the undone LSNs to stdout.

Lastly, it will write the resulting disk pages to ./disk_pages_after.json

**Execution Instructions**
To run in cli: python3 aeries.py

**Test Execution Instructions**
To run tests w/ bash: ./test.sh

or run tests w/o bash: python3 -m unittest discover tests
