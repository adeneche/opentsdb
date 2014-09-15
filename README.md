Fast import
===========

The import2 command enables bulk loading of time series data into OpenTSDB. It is based on the [import](http://opentsdb.net/docs/build/html/user_guide/cli/import.html) CLI tool but uses an advanced cached batching mechanism to speedup the loading. You provide one or more files and the tool will parse and load the data. Data must be formatted in the Telnet put style with one data point per line in a text file. Each file may be optionally be compressed with GZip and if so, must end with the .gz extension.

### Extra features
The tool also supports replication and repeating of the file data so we can load lots of metrics from a single file. Specifically, this tool supports the following scenarios:

#### Duplication
generate n duplicates of the same dataset aligned in time and put the duplicate number in a tag that is in addition to the one given in the dataset.

Example loading 'file1' and duplicating each data points 5 times using an extra tag 'mytag':
```
import2 --duplicate=mytag:5 file1
```

#### Repetition
generate k repeats of the same dataset concatenated in time (i.e. the repeats are laid one after another). This can be combined with the **duplication**.  Total amount of data should be k x n as big as the original file.

Example
```Shell
import2 --repeat=2 file1
```

#### Concatenation
read m files and concatenate them in time.  This should be possible to combine with **duplication** and **repetition**.  The tags in the files should be passed verbatim.

Example
```Shell
import2 file1 file2 file3
```

## Parameters

```Shell
import2 [--repeat=N] [--duplicate=TAG:K] [--noimport] [--print] [--mem] path [more paths]
```


Name      | Data Type      | Description | Default | Example |
----------|----------------|-------------|---------|---------|
repeat    | Integer        | repeat all concatenated files N times | 1 | 2 |
duplicate | String:Integer | creates K duplicates for each data point, each duplicate i has an extra "TAG=i" tag pair | 1 | cpu:3 |
noimport  | Boolean        | do not import data to TSDB | false | |
print     | Boolean        | print generated data to console | false | |
mem       | Boolean        | print memory usage | false | |
path      | String         | path to files to be imported. May be absolute or relative | |

The tool also supports all common command line parameters discribed [here](http://opentsdb.net/docs/build/html/user_guide/cli/index.html#common-parameters)

Example
Example
```Shell
import2 --repeat=2 --duplicate=cpu:3 file1 file2 file3
```

This command will concatenate all 3 files. Repeat all data points 2x in time and duplicate each data point 3x using an extra tag: "cpu=0", "cpu=1" and "cpu=2" for each duplicate.