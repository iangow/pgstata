# pgstata
Code to compile Stata add-in for accessing PostgreSQL data.

There is a branch `apple` with OS X-specific edits.

To compile

```
cd pgload-01
make
sudo make install
```

Copy the `.ado` file to the appropriate location (on OS X, `~/Library/Application\ Support/Stata/ado/personal`).

For examples of usage, please see [my blog](https://iangow.wordpress.com/2014/05/20/pulling-data-into-stata-from-postgresql-in-mac-os-x/).

