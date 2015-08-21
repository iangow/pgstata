
## 1. Install iODBC

Using MacPorts:
```
sudo port install libiodbc
```

## 2. Get psqlODBC

The following assumes a MacPorts installation of PostgreSQL. Omit the `--with-libpq=/opt/local/bin/pg_config` part if this does not apply to you. (You might have to do `sudo port select postgresql postgresql94` or use `--with-libpq=/opt/local/lib/postgresql94/bin/pg_config` if the configuration script still can't find `pg_config`.)

```
wget https://ftp.postgresql.org/pub/odbc/versions/src/psqlodbc-09.03.0400.tar.gz
tar xzf psqlodbc-09.03.0400.tar.gz 
cd psqlodbc-09.03.0400
./configure --with-iodbc --enable-pthreads --with-libpq=/opt/local/bin/pg_config
make
sudo make install
```

## 3. Install ODBC administrator and set up DSN

Get [ODBC Administrator Tool for Mac OS X v1.0](https://support.apple.com/kb/DL895?locale=en_US).
Open ODBC Administrator and set up the driver:

![](driver.png?raw=true)

and then the "User DSN":

![](user_dsn.png?raw=true)

