//
//  sumSquare.h
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
// Sample object for calculating sum of squares of odd and even integers via MapReduce
//

#ifndef parallelCalc_sumSquare_h
#define parallelCalc_sumSquare_h

#include "shim.h"

#include <iostream>
#include <map>
#include <vector>

#if USE_THREADS
#include <thread>
#endif

#include "calc.h"

// ========== Class Declarations ===================================================================

class SumSquare : public Calc {
public:
    SumSquare();

    // name of calculation, in a form usable as a directory name
    virtual std::string name() { return "sumSquare"; };
    
    // write key/value data usable as input to map operation
    virtual int startWorker(int nrows, std::ostream& output);
    
    // read key/value starting data, write mapped data
    virtual int mapWorker(std::istream& input, std::ostream& output);
    
    // read key/value mapped data, write reduced data
    virtual int reduceWorker(std::istream& input, std::ostream& output);
    
    // handle start | map | reduce calculations directly, without writing to and
    // reading from intermediate text strings
    virtual int singleThreadDirect(int nrows, std::ostream& output);
    
    // split map and reduce calculations over multiple threads
    virtual int multiThread(int nrows, int nthreads, std::ostream& output);

protected:
    typedef unsigned long StartValue;   // value type of starting data
    typedef unsigned long MappedValue;  // value type of mapped data
    typedef unsigned long ReducedValue; // value type of reduced data
    
    // write starting data as vector of key-value pairs
    virtual void start(int nrows, std::vector< std::pair<std::string, StartValue> >& startPairs);
    
    // read a range starting data from vector of key-value pairs, append mapped data to a multimap
    virtual void mapRange(
        const std::vector< std::pair<std::string, StartValue> >::const_iterator& beginStartValues,
        const std::vector< std::pair<std::string, StartValue> >::const_iterator& endStartValues,
        std::multimap<std::string, MappedValue>& mappedValues);
    
    // map a single key-value pair, append mapped data to a multimap
    virtual void mapOne(const std::string& keyIn, StartValue valueIn,
                     std::multimap<std::string, MappedValue>& mappedValues);
    
    // read a range of mapped data from a multimap, append reduced data to a multimap; the range of
    // mapped data must include ALL values for a key if ANY values for that key are included
    virtual void reduceRange(
        const std::multimap<std::string, MappedValue>& mappedPairs,
        const std::multimap<std::string, MappedValue>::const_iterator& beginMappedPairs,
        const std::multimap<std::string, MappedValue>::const_iterator& endMappedPairs,
        std::multimap<std::string, ReducedValue>& reducedPairs);

    // reduce values for a particular key; the range of mapped values must include all the values
    // for the specified key
    virtual void reduce(
        const std::string& keyMapped,
        std::multimap<std::string, MappedValue>::const_iterator& beginMappedValues,
        std::multimap<std::string, MappedValue>::const_iterator& endMappedValues,
        std::vector<ReducedValue>& reducedValues);

#if USE_THREAD
protected:
    std::vector<std::thread> threads;
#endif
};

// ========== Function Headers =====================================================================

// component tests
void ctest_sumSquare(int& totalPassed, int& totalFailed, bool useHadoop, bool verbose);

// code coverage
void cover_sumSquare(bool useHadoop, bool verbose);

#endif
