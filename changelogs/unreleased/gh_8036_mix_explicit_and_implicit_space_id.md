## bugfix/core

* Fixed a problem when space creation failed with duplication error when
  explicit and implicit space id were mixed. Now, occupied ids are skipped
  when schema max id is updated (gh-8036).
