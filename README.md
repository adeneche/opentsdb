Fast import
===========

The import2 command enables bulk loading of time series data into OpenTSDB. It is based on the [import](http://opentsdb.net/docs/build/html/user_guide/cli/import.html) CLI tool but uses an advanced cached batching mechanism to speedup the loading. You provide one or more files and the tool will parse and load the data. Data must be formatted in the Telnet put style with one data point per line in a text file. Each file may be optionally be compressed with GZip and if so, must end with the .gz extension.

## Parameters

```Shell
import2 [--noimport] [--print] path [more paths]
```


Name      | Data Type      | Description | Default | Example |
----------|----------------|-------------|---------|---------|
noimport  | Boolean        | do not import data to TSDB | false | |
print     | Boolean        | print generated data to console | false | |
path      | String         | path to files to be imported. May be absolute or relative | |

The tool also supports all common command line parameters discribed [here](http://opentsdb.net/docs/build/html/user_guide/cli/index.html#common-parameters)

Example
Example
```Shell
import2 --auto-metric file1 file2 file3
```