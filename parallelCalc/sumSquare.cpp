//
//  sumSquare.cpp
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

#include "sumSquare.h"

#include <cmath>
#include <iostream>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "callWithFork.h"
#include "utils.h"

using namespace std;

// ========== Classes ==============================================================================

SumSquare::SumSquare()
{
}

// write key/value data usable as input to map operation
int SumSquare::startWorker(int nrows, std::ostream& output)
{
    // create input data
    vector< pair<string, StartValue> > startPairs;
    start(nrows, startPairs);
    
    // write data
    for (size_t k = 0; k < startPairs.size(); k++) {
        output << startPairs[k].first << "\t" << startPairs[k].second << endl;
    }

    return 0;
}

// read key/value starting data, write mapped data
int SumSquare::mapWorker(std::istream& input, std::ostream& output)
{
    bool valid = true;
    while (!input.eof() && valid) {
        // read next row
        string startKey;
        StartValue startValue;
        valid = readKeyValue<StartValue>(input, startKey, startValue);
        
        if (valid) {
            // calculate
            multimap<string, MappedValue> mappedValues;
            mapOne(startKey, startValue, mappedValues);
            
            // write mapped row
            for (multimap<string, MappedValue>::const_iterator iter = mappedValues.begin();
                 iter != mappedValues.end();
                 iter++) {
                
                output << iter->first << "\t" << iter->second << endl;
            }
        }
    }
    
    return 0;
}

// read key/value mapped data, write reduced data
int SumSquare::reduceWorker(std::istream& input, std::ostream& output)
{
    multimap<string, MappedValue> mappedPairs;
    
    // accumulate & sort
    bool valid = true;
    while (!input.eof() && valid) {
        // read next row
        string mappedKey;
        MappedValue mappedValue;
        valid = readKeyValue<MappedValue>(input, mappedKey, mappedValue);
        
        if (valid) {
            mappedPairs.insert(make_pair(mappedKey, mappedValue));
        }
    }
    
    valid = input.eof();
    
    // call reduce for each key
    multimap<string, MappedValue>::const_iterator iterMapped = mappedPairs.begin();
    while (iterMapped != mappedPairs.end()) {
        // next key
        const string& mappedKey = iterMapped->first;
        
        // get range for next key
        pair<   multimap<string, MappedValue>::const_iterator,
                multimap<string, MappedValue>::const_iterator> range;
        
        range = mappedPairs.equal_range(mappedKey);
        
        // reduce over next range
        vector<ReducedValue> reducedValues;
        reduce(iterMapped->first, range.first, range.second, reducedValues);
        
        // write reduced rows
        vector<ReducedValue>::const_iterator iterReduced = reducedValues.begin();
        while (iterReduced != reducedValues.end() && valid) {
            valid = writeKeyValue<ReducedValue>(output, mappedKey, *iterReduced);
            
            iterReduced++;
        }
        
        iterMapped = range.second;
    }
    
    return 0;
}

// handle start | map | reduce calculations directly, without writing to and
// reading from intermediate text strings
int SumSquare::singleThreadDirect(int nrows, std::ostream& output)
{
    // start
    vector< pair<string, StartValue> > startPairs;
    start(nrows, startPairs);
    
    // map
    multimap<string, MappedValue> mappedPairs;
    mapRange(startPairs.begin(), startPairs.end(), mappedPairs);
    
    // reduce
    multimap<string, ReducedValue> reducedPairs;
    reduceRange(mappedPairs, mappedPairs.begin(), mappedPairs.end(), reducedPairs);
    
    // output
    multimap<string, ReducedValue>::const_iterator iterOut = reducedPairs.begin();
    bool valid = true;
    while (iterOut != reducedPairs.end() && valid) {
        valid = writeKeyValue<ReducedValue>(output, iterOut->first, iterOut->second);
        
        iterOut++;
    }
    
    return valid ? 0 : 1;
}

// split map and reduce calculations over multiple threads
int SumSquare::multiThread(int nrows, int nthreads, std::ostream& output)
{
#if USE_THREADS
    // start
    vector< pair<string, StartValue> > startPairs;
    start(nrows, startPairs);
    
    // can't have more map threads than rows
    int mapThreadCount = nthreads;
    if (mapThreadCount > nrows) {
        mapThreadCount = nrows;
    }
    
    // divide up work among map threads
    vector< vector< pair<string, StartValue> >::const_iterator > startIters;
    startIters.push_back(startPairs.begin());
    
    for (int k = 1; k < mapThreadCount; k++) {
        int offset = (int)round(k * nrows / (double)mapThreadCount);
        
        startIters.push_back(startPairs.begin() + offset);
    }
    
    startIters.push_back(startPairs.end());

#undef DEBUG_WITHOUT_THREADS

    // map
    vector<thread> mapThreads;
    vector< multimap<string, MappedValue> > mappedPairsVector(mapThreadCount);
    for (int k = 0; k < mapThreadCount; k++) {
#ifdef DEBUG_WITHOUT_THREADS
        mapRange(startIters[k], startIters[k + 1], mappedPairsVector[k]);
        
#else
        mapThreads.push_back(
            thread(bind(&SumSquare::mapRange,
                        this,
                        ref(startIters[k]),
                        ref(startIters[k + 1]),
                        ref(mappedPairsVector[k]))
                   ));
#endif
    }

#ifndef DEBUG_WITHOUT_THREADS
    // join threads
    for (int k = 0; k < mapThreadCount; k++) {
        mapThreads[k].join();
    }
#endif
    
    // join results
    multimap<string, MappedValue> mappedPairs;
    for (int k = 0; k < mapThreadCount; k++) {
        mappedPairs.insert(mappedPairsVector[k].begin(), mappedPairsVector[k].end());
    }
    
    // count mapped keys
    int numMappedKeys = 0;
    multimap<string, MappedValue>::const_iterator iterMapped = mappedPairs.begin();
    while (iterMapped != mappedPairs.end()) {
        numMappedKeys++;

        pair<   multimap<string, MappedValue>::const_iterator,
                multimap<string, MappedValue>::const_iterator> range;
        
        range = mappedPairs.equal_range(iterMapped->first);
        iterMapped = range.second;
    }

    // can't have more reduce threads than keys
    int reduceThreadCount = nthreads;
    if (reduceThreadCount > numMappedKeys) {
        reduceThreadCount = numMappedKeys;
    }
    
    // divide up work among reduce threads
    vector< multimap<string, MappedValue>::const_iterator > mappedIters;
    mappedIters.push_back(mappedPairs.begin());
    
    iterMapped = mappedPairs.begin();
    int keyCount = 0;
    for (int k = 1; k < reduceThreadCount; k++) {
        int nextKeyCount = (k * numMappedKeys + reduceThreadCount / 2) / reduceThreadCount;
        
        while (keyCount < nextKeyCount) {
            keyCount++;
            
            pair<   multimap<string, MappedValue>::const_iterator,
                    multimap<string, MappedValue>::const_iterator> range;
            
            range = mappedPairs.equal_range(iterMapped->first);
            iterMapped = range.second;
        }
        
        mappedIters.push_back(iterMapped);
    }

    mappedIters.push_back(mappedPairs.end());

    // reduce
    vector<thread> reduceThreads;
    vector< multimap<string, ReducedValue> > reducePairsVector(reduceThreadCount);
    for (int k = 0; k < reduceThreadCount; k++) {
#ifdef DEBUG_WITHOUT_THREADS
        reduceRange(mappedPairs, mappedIters[k], mappedIters[k + 1], reducePairsVector[k]);
        
#else
        reduceThreads.push_back(
                             thread(bind(&SumSquare::reduceRange,
                                         this,
                                         ref(mappedPairs),
                                         ref(mappedIters[k]),
                                         ref(mappedIters[k + 1]),
                                         ref(reducePairsVector[k]))
                                    ));
#endif
    }
    
#ifndef DEBUG_WITHOUT_THREADS
    // join threads
    for (int k = 0; k < reduceThreadCount; k++) {
        reduceThreads[k].join();
    }
#endif
    
    // join results
    multimap<string, ReducedValue> reducedPairs;
    for (int k = 0; k < reduceThreadCount; k++) {
        reducedPairs.insert(reducePairsVector[k].begin(), reducePairsVector[k].end());
    }

    // output
    multimap<string, ReducedValue>::const_iterator iterOut = reducedPairs.begin();
    bool valid = true;
    while (iterOut != reducedPairs.end() && valid) {
        valid = writeKeyValue<ReducedValue>(output, iterOut->first, iterOut->second);
        
        iterOut++;
    }

#endif
    return 0;
}

// write starting data as vector of key-value pairs
void SumSquare::start(int nrows, std::vector< std::pair<std::string, StartValue> >& startPairs)
{
    for (int k = 1; k <= nrows; k++) {
        if (k % 2 == 0) {
            startPairs.push_back(make_pair("EVEN", k));
            
        } else {
            startPairs.push_back(make_pair("ODD ", k));
        }
    }
}

// read a range starting data from vector of key-value pairs, append mapped data to a multimap
void SumSquare::mapRange(
    const vector< pair<string, StartValue> >::const_iterator& beginStartValues,
    const vector< pair<string, StartValue> >::const_iterator& endStartValues,
    multimap<string, MappedValue>& mappedValues)
{
    vector< pair<string, StartValue> >::const_iterator iter = beginStartValues;
    while (iter != endStartValues) {
        mapOne(iter->first, iter->second, mappedValues);
        
        iter++;
    }
}

// map a single key-value pair, append mapped data to a multimap
void SumSquare::mapOne(const std::string& keyIn, StartValue valueIn,
                       std::multimap<std::string, MappedValue>& mappedValues)
{
    mappedValues.insert(make_pair(keyIn, valueIn * valueIn));
    
    if (delay != 0) {
        sleepFor(delay);
    }
}

// read a range of mapped data from a multimap, append reduced data to a multimap; the range of
// mapped data must include ALL values for a key if ANY values for that key are included
void SumSquare::reduceRange(
    const std::multimap<std::string, MappedValue>& mappedPairs,
    const std::multimap<std::string, MappedValue>::const_iterator& beginMappedPairs,
    const std::multimap<std::string, MappedValue>::const_iterator& endMappedPairs,
    std::multimap<std::string, ReducedValue>& reducedPairs)
{
    multimap<string, MappedValue>::const_iterator iterMapped = beginMappedPairs;
    while (iterMapped != endMappedPairs) {
        // next key
        const string& mappedKey = iterMapped->first;
        
        // get range for next key
        pair<   multimap<string, MappedValue>::const_iterator,
                multimap<string, MappedValue>::const_iterator> range;
        
        range = mappedPairs.equal_range(mappedKey);
        
        // reduce over next range
        vector<ReducedValue> reducedValues;
        reduce(mappedKey, range.first, range.second, reducedValues);
        
        // write reduced rows
        vector<ReducedValue>::const_iterator iterReduced = reducedValues.begin();
        while (iterReduced != reducedValues.end()) {
            reducedPairs.insert(make_pair(mappedKey, *iterReduced));
            
            iterReduced++;
        }
        
        iterMapped = range.second;
    }
}

// reduce values for a particular key; the range of mapped values must include all the values
// for the specified key
void SumSquare::reduce(const std::string& keyMapped,
                    std::multimap<std::string, MappedValue>::const_iterator& beginMappedValues,
                    std::multimap<std::string, MappedValue>::const_iterator& endMappedValues,
                    std::vector<ReducedValue>& reducedValues)
{
    map<string, ReducedValue> mapSum;
    
    multimap<string, MappedValue>::const_iterator iterMapped = beginMappedValues;
    while (iterMapped != endMappedValues) {
        map<string, ReducedValue>::iterator iterReduced = mapSum.find(iterMapped->first);
        
        if (iterReduced == mapSum.end()) {
            // first value for new key
            mapSum.insert(make_pair(iterMapped->first, iterMapped->second));
            
        } else {
            // sum
            iterReduced->second += iterMapped->second;
        }

        iterMapped++;
    }
    
    for (map<string, ReducedValue>::const_iterator iter = mapSum.begin();
         iter != mapSum.end();
         iter++) {
        
        reducedValues.push_back(iter->second);
    }
}


// ========== Tests ================================================================================

// component tests
void ctest_sumSquare(int& totalPassed, int& totalFailed, bool useHadoop, bool verbose)
{
    int passed = 0;
    int failed = 0;
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::startWorker
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::mapWorker
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::reduceWorker
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::singleThreadDirect

    {
        SumSquare sumSquare;
        
        int nrows = 10;
        ostringstream oss;
        int status = sumSquare.singleThreadDirect(nrows, oss);
        string outStr = oss.str();
        const string expected = "EVEN\t220\nODD \t165\n";
        
        if (status == 0 && outStr == expected) passed++; else failed++;
    }
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::multiThread
    
#if USE_THREADS
    {
        SumSquare sumSquare;
        
        int nrows = 10;
        int nthreads = 2;
        ostringstream oss;
        int status = sumSquare.multiThread(nrows, nthreads, oss);
        string outStr = oss.str();
        const string expected = "EVEN\t220\nODD \t165\n";
        
        if (status == 0 && outStr == expected) passed++; else failed++;
    }
#endif
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::start
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::mapRange
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // Calc::singleThreadWorkers
    
    {
        SumSquare sumSquare;
        
        int nrows = 10;
        ostringstream oss;
        int status = sumSquare.singleThreadWorkers(nrows, oss);
        string outStr = oss.str();
        const string expected = "EVEN\t220\nODD \t165\n";
        
        if (status == 0 && outStr == expected) passed++; else failed++;
    }
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // Calc::forkWorkers

    {
        SumSquare sumSquare;
        
        int nrows = 10;
        ostringstream oss;
        int status = sumSquare.forkWorkers(nrows, oss);
        string outStr = oss.str();
        const string expected = "EVEN\t220\nODD \t165\n";
        
        if (status == 0 && outStr == expected) passed++; else failed++;
    }

    // ~~~~~~~~~~~~~~~~~~~~~~
    // Calc::hadoop
    
    if (useHadoop) {
        SumSquare sumSquare;
        
        int nrows = 10;
        ostringstream oss;
        int status = sumSquare.hadoop(nrows, oss);
        string outStr = oss.str();
        const string expected = "EVEN\t220\nODD \t165\n";
        
        if (status == 0 && outStr == expected) passed++; else failed++;
    }
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    
    if (verbose) {
        cerr << "sumSquare.cpp" << "\t\t" << passed << " passed, " << failed << " failed" << endl;
    }
    
    totalPassed += passed;
    totalFailed += failed;
}

// code coverage
void cover_sumSquare(bool useHadoop, bool verbose)
{
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::startWorker

    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::mapWorker

    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::reduceWorker
    
    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::singleThreadDirect

    {
        SumSquare sumSquare;
        
        int nrows = 10;
        ostringstream oss;
        sumSquare.singleThreadDirect(nrows, oss);
    }

    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::multiThread
    
#if USE_THREADS
    {
        SumSquare sumSquare;
        
        int nrows = 10;
        int nthreads = 2;
        ostringstream oss;
        sumSquare.multiThread(nrows, nthreads, oss);
    }
    
    {
        SumSquare sumSquare;
        
        int nrows = 4;
        int nthreads = 5;
        ostringstream oss;
        sumSquare.multiThread(nrows, nthreads, oss);
    }
#endif

    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::start

    // ~~~~~~~~~~~~~~~~~~~~~~
    // SumSquare::mapRange

    // ~~~~~~~~~~~~~~~~~~~~~~
    // Calc::singleThreadWorkers

    {
        SumSquare sumSquare;
        
        int nrows = 10;
        sumSquare.setDelay(100);
        sumSquare.setVerbose(true);
        ostringstream oss;
        sumSquare.singleThreadWorkers(nrows, oss);
    }

    // ~~~~~~~~~~~~~~~~~~~~~~
    // Calc::forkWorkers

    {
        SumSquare sumSquare;
        
        int nrows = 10;
        sumSquare.setVerbose(true);
        ostringstream oss;
        sumSquare.forkWorkers(nrows, oss);
    }

    // ~~~~~~~~~~~~~~~~~~~~~~
    // Calc::hadoop
    
    if (useHadoop) {
        SumSquare sumSquare;
        
        int nrows = 10;
        sumSquare.setVerbose(true);
        ostringstream oss;
        sumSquare.hadoop(nrows, oss);
    }

}
