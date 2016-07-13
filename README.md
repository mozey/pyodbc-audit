# pyodbc-audit

Tool for extracting information from unfamiliar databases


## Example Usage

### Quickstart

Install SQLite sample database 
[Chinook_Sqlite.sqlite](https://chinookdatabase.codeplex.com/) 
in same folder as audit.py
    
List tables in default target database
 
    python ./audit.py -l


### Audit database changes

Run audit to get initial database state

    python ./audit.py -a
    
Change something in the database and run audit again 
    
View results in `audit.db`

    select * from audit
    order by modified desc;
    
    
### Reset audit

Changes are audited relative to a snapshot.
The snapshot is created the first time the audit script is executed.
To reset the snapshot run

    delete from audit
    
Doing above is preferable to just removing `audit.db`,
doing this might confuse open sqlite clients


### Using a config file to override the defaults
    
The target database can be set to an ODBC data source.

Create a file `config.json` at the same path as `audit.py`, for example

    {
        "target": {
            "type": "odbc",
            "connection_string": "Driver={SQL Server};Server=localhost;Database=MyDatabase;Uid=sa;Pwd=xxx;"
        }
    }


### [pyodbc on OSX](https://gist.github.com/Bouke/10454272)

Install libraries first

    brew install unixodbc
    brew install freetds --with-unixodbc
    
Then install pyodbc

    pip install pyodbc
    
FreeTDS should work without configuration

    tsql -S [IP or hostname] -U [username] -P [password]
    
To create a DSN link the drivers in `/usr/local/etc/odbcinst.ini`

    [FreeTDS]
    Description = TD Driver (MSSQL)
    Driver = /usr/local/lib/libtdsodbc.so
    Setup = /usr/local/lib/libtdsodbc.so
    FileUsage = 1
    
Configure the DSN in `/usr/local/etc/odbc.ini`

    [MYDSN]
    Driver = FreeTDS
    Server = [IP address]
    Port = 1433

Using the DSN

    isql MYDSN [username] [password] -v
    





