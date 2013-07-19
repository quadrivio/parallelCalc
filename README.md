## parallelCalc

---

### Description

This project contains a sample framework to develop, test and compare C++ MapReduce
calculations handled by 1) a single-threaded process, 2) a multi-threaded process, or
3) streaming Hadoop. It is intended as example code for those who plan on modifying
it for their own purposes.

The project was developed for Mac OS X 10.6 or 10.7 using XCode. The files compile and run
under Visual Studio on Windows 7 and Eclipse on CentOS 6.4, as well, but have not been 
fully tested and are still in progress on those platforms.

A single-threaded process running in an IDE is convenient for testing and debugging the
MapReduce calculations.

A multi-threaded process on a multi-core system is likely to be the most efficient when
only one computer is available, since it avoids the overhead involved in starting up and
communicating with Hadoop. Multi-threading is not available for the OS X 10.6 version.

When many computers are available, then Hadoop has the potential to bring the most power
to bear on the calculations. By using the streaming Hadoop facility, it is possible to
write the MapReduce calculations in C++ instead of Java; the map and reduce code is
encapsulated in a command-line tool, which is then invoked by the Hadoop system. See
<http://hadoop.apache.org/docs/stable/streaming.html>

### Usage

Under OS X 10.7, the project produces a tool named parallelCalct (threading available).
Under OS X 10.6, the project produces a tool named parallelCalcn (threading not 
available).

To implement a MapReduce calculation, subclass the Calc class and fill in the appropriate
methods. An example, the SumSquare class, is included in the project. After the
command-line tool is built, the MapReduce pattern can be invoked manually on the
command line by piping the tool with the following options:

On OS X 10.7:
```
parallelCalct -start -n <nrows> | parallelCalct -map | parallelCalct -reduce
```

On OS X 10.6:
```
parallelCalcn -start -n <nrows> | parallelCalcn -map | parallelCalcn -reduce
```

where <nrows> is the number of rows of test data to generate.

To run the calculation via Hadoop, use `parallelCalct -n <nrows> -hadoop` or 
`parallelCalcn -n <nrows> -hadoop`

To run via multiple threads, use `parallelCalct -n <nrows> -threads <nthreads>`

To run tests, use `parallelCalct -test` or `parallelCalcn -test`. Options that can be used
with `-test` are `-v` for verbose and `-hadoop` to include calls to hadoop.

These options and others can also be run directly from XCode. At the top of the XCode
window, click parallelCalc and select Edit Scheme. Select Run parallelCalc at the left of
the dialog box and then enter the desired options in the box labeled, "Arguments
Passed On launch."

### Development Environments

```
Max OS 10.6 
XCode 4.2
c++0x with libstdc++
Hadoop 1.1.2
(no multi-threading) 
```

```
Mac OS X 10.7
XCode 4.6
c++0x with libc++
Hadoop 1.1.2
```

Work in progress:

```
Windows 7
Visual Studio C/C++ Express 2012
(no Hadoop used)
```

```
CentOS 6.4
gcc 4.4
Eclipse Kepler
Hadoop 1.1.2
```

### License

BSD
