# pgstata
Code to compile Stata add-in for accessing PostgreSQL data.

There is a branch `apple` with OS X-specific edits.

To compile, enter the following on the command line:

```
cd pgload-01
make
sudo make install
```

Copy the `.ado` file to the appropriate location (on OS X, `~/Library/Application\ Support/Stata/ado/personal`).

For examples of usage, please see [my blog](https://iangow.wordpress.com/2014/05/20/pulling-data-into-stata-from-postgresql-in-mac-os-x/).

Note that the files from Stata and Oxford were obtained as follows:

```
wget http://code.ceu.ox.ac.uk/stata/pgload-0.1.tar.gz
tar -zxvf pgload-0.1.tar.gz
rm pgload-0.1.tar.gz
wget http://www.stata.com/plugins/stplugin.c
wget http://www.stata.com/plugins/stplugin.h
```
