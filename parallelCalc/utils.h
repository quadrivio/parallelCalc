//
//  utils.h
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

#ifndef entree_utils_h
#define entree_utils_h

#include "shim.h"

#include <string>
#include <vector>

// ========== Macros ===============================================================================

#ifndef CERR
#define CERR std::cerr
#endif

#ifndef LOGIC_ERROR_IF
#define LOGIC_ERROR_IF(x, msg) if (x) { throw_logic_error(__FILE__, __LINE__, msg); }
#endif

#ifndef RUNTIME_ERROR_IF
#define RUNTIME_ERROR_IF(x, msg) if (x) { throw_runtime_error(__FILE__, __LINE__, msg); }
#endif

#ifdef DEBUG
// for debugging; provides location at which to set a breakpoint
#define SKIP noop();
#else
#define SKIP
#endif

// ========== Function Headers =====================================================================

// throw std::logic_error with custom message include source file name and line number
void throw_logic_error(const std::string& path, int line, const std::string& msg);

// throw std::runtime_error with custom message; include source file name and line number if DEBUG
// is defined
void throw_runtime_error(const std::string& path, int line, const std::string& msg);

// convert time_t to local time string in ISO format
std::string localTimeString(const time_t t);

// return true if entire string is parsable as number
bool isNumeric(const std::string str);

// return value from parsing string as double
double toDouble(const std::string str);

// return value from parsing string as long
long toLong(const std::string str);

// write string to file
void stringToFile(const std::string& str, const std::string& path);

// read string from file
void fileToString(const std::string& path, std::string& str);

// return current working directory
std::string getWorkingDirectory();

// return error message for bad path
std::string badPathErrorMessage(const std::string& file);

// wrapper for sleep functions
void sleepFor(int milliseconds);

// for debugging and testing; epoch time in milliseconds
long long millisecondTime();

// for debugging and testing; create directory
void makeDir(const std::string& path);

// for debugging and testing; remove directory
void removeDir(const std::string& path);

// for debugging and testing; location at which to set a breakpoint
void noop();

#if WINDOWS
double round(double x);
#endif

// component tests
void ctest_utils(int& totalPassed, int& totalFailed, bool verbose);

// code coverage
void cover_utils(bool verbose);

#endif
