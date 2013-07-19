//
//  test.cpp
//  parallelCalc
//
//  Created by MPB on 7/18/13.
//  Copyright (c) 2013 Quadrivio Corporation. All rights reserved.
//
//  License http://opensource.org/licenses/BSD-2-Clause
//          <YEAR> = 2013
//          <OWNER> = Quadrivio Corporation
//

//
// Component, code coverage, and integration tests
//

#include "test.h"

#include <iostream>

#include "callWithFork.h"
#include "sumSquare.h"
#include "utils.h"

using namespace std;

// ========== Local Headers ========================================================================

bool test_commandLine(bool verbose);

// ========== Functions ============================================================================

void test(bool useHadoop, bool verbose)
{
    if (verbose) {
        cerr << "Testing" << endl;
        cerr << "Working directory: " << getWorkingDirectory() << endl;
    }
    
    // ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 
    // Component tests
    
    if (verbose) {
        cerr << endl << "Component tests" << endl;
    }
    
    int totalPassed = 0;
    int totalFailed = 0;
    
    ctest_callWithFork(totalPassed, totalFailed, verbose);
    ctest_sumSquare(totalPassed, totalFailed, useHadoop, verbose);
    ctest_utils(totalPassed, totalFailed, verbose);
    
    if (verbose) {
        cerr << "Total" << "\t\t\t\t" << totalPassed << " passed, " << totalFailed << " failed" << endl;
    }
    
    // ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 
    // Code coverage
    
    if (verbose) {
        cerr << endl << "Code coverage" << endl;
    }
    
    cover_callWithFork(verbose);
    cover_sumSquare(useHadoop, verbose);
    cover_utils(verbose);
    
    // ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 
    // Integration
    
    if (verbose) {
        cerr << endl << "Integration tests" << endl;
    }
    
    if (test_commandLine(verbose)) {
        totalPassed++;
        
    } else {
        cerr << "test_commandLine() failed" << endl;
        totalFailed++;
    }
    
    // ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ 
    
    cerr << "------------" << endl;
    
    if (totalFailed > 0) {
        cerr << "Tests failed" << endl;
        
    } else {
        cerr << "Tests OK" << endl;
    }
}

// ========== Local Functions ======================================================================

// used in test_commandLine()
int main (int argc, const char * argv[]);

// test command-line interface
bool test_commandLine(bool verbose)
{
    int result = 0;
    
    {
        int argc = 6;
        const char *argv[] = {
            (char *)"parallelCalc",
            
            (char *)"-n",
            (char *)"10",
            
            (char *)"-d",
            (char *)"100",

            (char *)"-v"
        };
        
        result = main(argc, argv);
    }
    
    return result == 0;
}

