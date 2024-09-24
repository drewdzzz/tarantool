## bugfix/box

* Fixed a crash when the first of two consequent DDL operations got
  rolled back due to a WAL failure and the second one was in progress.
  Now potentially yielding DDL operations (ones that involve index build
  or format check) return an error when there is another modification of
  the same space being committed (gh-10235).
