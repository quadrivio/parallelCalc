//
//  callWithFork.h
//  parallelCalc
//
//  Created by MPB on 7/4/13.
//  Copyright (c) 2013 Quadrivio Corporation. All rights reserved.
//
//  License http://opensource.org/licenses/BSD-2-Clause
//          <YEAR> = 2013
//          <OWNER> = Quadrivio Corporation
//

//
// Functions to fork and call a command-line tool, pipe stdin, stdout, sterr to the tool, and
// wait for the tool to complete
//

#ifndef parallelCalc_callWithFork_h
#define parallelCalc_callWithFork_h

#include "shim.h"

#include <iostream>
#include <string>
#include <vector>

// ========== Function Headers =====================================================================

// fork process, call command-line tool with specified arguments, pipe stdin, stdout, sterr, wait
// for completion; returns 0 for success; assumes moderate amount of input and output
int forkPipeWait(const std::string& path, std::vector<std::string> args, std::istream& input,
                  std::ostream& output, std::ostream& error);

// convenience function: call forkPipeWait with empty input and variable number of arguments, return
// stdout and sterr results as strings; returns 0 for success
int callTool(const std::string& toolName, const std::string& toolPath, std::string& stdoutStr, 
              std::string& stderrStr, bool logging, const char *arg, ...);

// component tests
void ctest_callWithFork(int& totalPassed, int& totalFailed, bool verbose);

// code coverage
void cover_callWithFork(bool verbose);

#endif
