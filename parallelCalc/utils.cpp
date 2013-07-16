//
//  utils.cpp
//  entree
//
//  Created by MPB on 5/8/13.
//  Copyright (c) 2013 Quadrivio Corporation. All rights reserved.
//  License http://opensource.org/licenses/BSD-2-Clause
//          <YEAR> = 2013
//          <OWNER> = Quadrivio Corporation
//

//
// Miscellaneous utility functions
//

#include <fstream>  // must precede .h includes

#include "utils.h"

#if WINDOWS
#include <Windows.h>
#include <direct.h>
#include <time.h>

#else
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#include <cmath>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

using namespace std;

// ========== Functions ============================================================================

// throw std::logic_error with custom message include source file name and line number
void throw_logic_error(const std::string& path, int line, const std::string& msg)
{
    string file;
    
#if WINDOWS
    size_t start = path.find_last_of("\\/");
    
#else
    size_t start = path.find_last_of("/");
#endif
    
    if (start == string::npos) {
        file = path;
        
    } else {
        file = path.substr(start + 1);
    }
    
    ostringstream oss;
    oss << msg << " at " << file << " line " << line;
    throw logic_error(oss.str());
}

// throw std::runtime_error with custom message; include source file name and line number if DEBUG
// is defined
void throw_runtime_error(const std::string& path, int line, const std::string& msg)
{
#ifdef DEBUG
    string file;
    
#if WINDOWS
    size_t start = path.find_last_of("\\/");
    
#else
    size_t start = path.find_last_of("/");
#endif
    
    if (start == string::npos) {
        file = path;
        
    } else {
        file = path.substr(start + 1);
    }
    
    CERR << msg << " at " << file << " line " << line << endl;
#endif
    
    throw runtime_error(msg);
}

// convert time_t to local time string in ISO format
string localTimeString(const time_t t)
{
    const size_t MAXSIZE = 256;
    char str[MAXSIZE];
    strftime(str, MAXSIZE, "%Y-%m-%d %H:%M:%S", localtime(&t));
    
    return string(str);
}

// return true if entire string is parsable as number
bool isNumeric(const std::string str)
{
    istringstream iss(str);
    double d;
    iss >> d;
    
    bool result = !iss.fail() && iss.eof();
    
    return result;
}

// return value from parsing string as double
double toDouble(const std::string str)
{
    istringstream iss(str);
    double d;
    iss >> d;

    return d;
}

// return value from parsing string as long
long toLong(const std::string str)
{
    istringstream iss(str);
    long k;
    iss >> k;
    
    return k;
}

// write string to file
void stringToFile(const std::string& str, const std::string& path)
{
    ofstream ofs(path.c_str());
    
    ofs << str;
    
    ofs.close();
}

// read string from file
void fileToString(const std::string& path, std::string& str)
{
    ifstream ifs(path.c_str());
    
    ostringstream oss;
    oss << ifs.rdbuf();
    
    str = oss.str();
    
    ifs.close();
}

// return current working directory
std::string getWorkingDirectory()
{
    const size_t BUFSIZE = 2048;
    char cwd[BUFSIZE];
    
#if WINDOWS
	if (_getcwd(cwd, BUFSIZE) == NULL)
        
#else
	if (getcwd(cwd, BUFSIZE) == NULL)
#endif
	{
        RUNTIME_ERROR_IF(true, "unable to get working directory");
    };

    return cwd;
}

// return error message for bad path
std::string badPathErrorMessage(const std::string& file)
{
    string msg = "bad path ";

#if WINDOWS
    msg += getWorkingDirectory() + "\\" +  file;
    
#else
    msg += getWorkingDirectory() + "/" + file;
#endif
    
    return msg;
}

// wrapper for sleep functions
void sleepFor(int milliseconds)
{
#if WINDOWS
    Sleep(milliseconds);
    
#else
    usleep(1000 * milliseconds);
#endif
}

// for debugging and testing; epoch time in milliseconds
long long millisecondTime()
{
    long long msec;
    
#if WINDOWS
    // number of 100-nanosecond intervals since January 1, 1601 (UTC).
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
    
    _ULARGE_INTEGER uli;
    uli.u.LowPart = ft.dwLowDateTime;
    uli.u.HighPart = ft.dwHighDateTime;
    msec = (uli.QuadPart + 10000LL / 2) / 10000LL;
    
#else
    // seconds and microseconds since the Epoch (00:00:00 UTC, January 1 1970)
    timeval tv;
    gettimeofday(&tv, NULL);
    msec = ((long long)tv.tv_sec * 1000000LL + tv.tv_usec + 1000LL / 2) / 1000LL;
#endif

    return msec;
}

// for debugging and testing; create directory
void makeDir(const std::string& path)
{
#if WINDOWS
	//wstring wpath(path.begin(), path.end());
    BOOL result = CreateDirectory(path.c_str(), NULL);
    RUNTIME_ERROR_IF(!result, "makeDir fail");
    
#else
    int result = mkdir(path.c_str(), 700);
    RUNTIME_ERROR_IF(result != 0, "makeDir fail");
#endif
}

// for debugging and testing; remove directory
void removeDir(const std::string& path)
{
#if WINDOWS
	//wstring wpath(path.begin(), path.end());
    BOOL result = RemoveDirectory(path.c_str());
    RUNTIME_ERROR_IF(!result, "removeDir fail");
    
#else
    int result = rmdir(path.c_str());
    RUNTIME_ERROR_IF(result != 0, "removeDir fail");
#endif
}

// for debugging and testing; location at which to set a breakpoint
void noop()
{
}

#if WINDOWS
double round(double x)
{
	return floor(x + 0.5);
}
#endif

// ========== Tests ================================================================================

// component tests
void ctest_utils(int& totalPassed, int& totalFailed, bool verbose)
{
    int passed = 0;
    int failed = 0;
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // throw_logic_error
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // throw_runtime_error
        
    // ~~~~~~~~~~~~~~~~~~~~~~
    // localTimeString
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // isNumeric
    
    if (isNumeric("123")) passed++; else failed++;
    if (isNumeric(" 12.3")) passed++; else failed++;
    if (isNumeric(".123")) passed++; else failed++;
    if (!isNumeric("123A")) passed++; else failed++;
    if (!isNumeric("B")) passed++; else failed++;
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // toDouble
    
    if ((int)toDouble("2.0") == 2) passed++; else failed++;
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // toLong
    
    if (toLong("128") == 128) passed++; else failed++;
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // stringToFile
    // fileToString
    
    remove("foo.txt");
    
    string outStr = "Hello World\nFoo\tBar";
    stringToFile(outStr, "foo.txt");
    
    string inStr;
    fileToString("foo.txt", inStr);
    
    if (outStr == inStr) passed++; else failed++;
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // getWorkingDirectory
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // badPathErrorMessage
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // sleepFor
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // millisecondTime
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // makeDir
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // removeDir
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // noop
    
    // for TDD purists:
    noop();
    passed++;
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // round
    
#if WINDOWS
#endif
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    
    if (verbose) {
        CERR << "utils.cpp" << "\t" << passed << " passed, " << failed << " failed" << endl;
    }
    
    totalPassed += passed;
    totalFailed += failed;
}

void cover_utils(bool verbose)
{
    // ~~~~~~~~~~~~~~~~~~~~~~
    // throw_logic_error
    
    try {
        throw_logic_error("foo", 1, "message A");
        
    } catch(logic_error e) {
        if (verbose) {
            CERR << "one \"message A\" error follows:" << endl;
            CERR << e.what() << endl;
        }
    }
    
#if WINDOWS
    string path = "foo\\bar";
    
#else
    string path = "foo/bar";
#endif
    
    try {
        throw_logic_error(path, 2, "message B");
        
    } catch(logic_error e) {
        if (verbose) {
            CERR << "one \"message B\" error follows:" << endl;
            CERR << e.what() << endl;
        }
    }
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // throw_runtime_error
    
#ifdef DEBUG
    CERR << "one \"message C\" error follows:" << endl;
#endif
    
    try {
        throw_runtime_error("foo", 3, "message C");
        
    } catch(runtime_error e) {
        if (verbose) {
            CERR << "one \"message C\" error follows:" << endl;
            CERR << e.what() << endl;
        }
    }
 
    // ~~~~~~~~~~~~~~~~~~~~~~
    // localTimeString
    
    time_t t;
    time(&t);
    string str = localTimeString(t);
    if (verbose) {
        CERR << str << endl;
    }
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // isNumeric
    
    isNumeric("123");
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // toDouble
    
    toDouble("1.23");
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // toLong
    
    toLong("123");
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // stringToFile
    
    stringToFile("foo", "foo.txt");
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // fileToString
    
    fileToString("foo.txt", str);
    remove("foo.txt");
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // getWorkingDirectory
    
    getWorkingDirectory();
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // badPathErrorMessage

    badPathErrorMessage("foo");

    // ~~~~~~~~~~~~~~~~~~~~~~
    // sleepFor
    
    sleepFor(100);
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // millisecondTime
    
    millisecondTime();
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // makeDir
    
    makeDir("foo");
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // removeDir
    
    removeDir("foo");

    // ~~~~~~~~~~~~~~~~~~~~~~
    // noop
    
    noop();

    // ~~~~~~~~~~~~~~~~~~~~~~
    // round
    
#if WINDOWS
    round(1.7);
#endif
    

}
