//
//  calc.h
//  parallelCalc
//
//  Created by MPB on 7/9/13.
//  Copyright (c) 2013 Quadrivio Corporation. All rights reserved.
//
//  License http://opensource.org/licenses/BSD-2-Clause
//          <YEAR> = 2013
//          <OWNER> = Quadrivio Corporation
//

//
// Functions and parent object for running MapReduce calculations via single thread, multiple
// threads, forked tools, and streaming Hadoop. The command-line equivalent of the calculation is: 
//
//      parallelCalcn -start | parallelCalcn -map | parallelCalcn -reduce
// or
//      parallelCalct -start | parallelCalct -map | parallelCalct -reduce
//
// where each tool outputs text consisting of one or more lines of the form:
//
//      <key> <tab character> <value>
//

#ifndef parallelCalc_calc_h
#define parallelCalc_calc_h

#include <iostream>
#include <limits>
#include <string>

// ========== Class Declarations ===================================================================

class Calc {
public:
    Calc();
    virtual ~Calc();
    
    // name of calculation, in a form usable as a directory name
    virtual std::string name() = 0;
    
    // if verbose flag is set, set progress info to stderr
    virtual void setVerbose(bool verbose) { this->verbose = verbose; };
    virtual bool isVerbose() { return verbose; };
    
    // for testing and debugging; add delay (in msec) to each mapping operation (each row)
    virtual void setDelay(int delay) { this->delay = delay; };
    virtual int getDelay() { return delay; };
    
    // override to write key/value data usable as input to map operation
    virtual int startWorker(int nrows, std::ostream& output);
    
    // override to read key/value starting data, write mapped data
    virtual int mapWorker(std::istream& input, std::ostream& output);
    
    // override to read key/value mapped data, write reduced data
    virtual int reduceWorker(std::istream& input, std::ostream& output);
    
    // call startWorker, mapWorker, reduceWorker in main thread, saving intermediate results to
    // strings for debugging
    virtual int singleThreadWorkers(int nrows, std::ostream& output);
    
    // override to handle start | map | reduce calculations directly, without writing to and
    // reading from intermediate text strings
    virtual int singleThreadDirect(int nrows, std::ostream& output);
    
    // for debugging and testing: fork and call via command-line:
    // parallelCalcn -start | parallelCalcn -map | parallelCalcn -reduce
    // or
    // parallelCalct -start | parallelCalct -map | parallelCalct -reduce
    virtual int forkWorkers(int nrows, std::ostream& output);
    
    // call parallelCalc -map and parallelCalc -reduce via Hadoop streaming
    virtual int hadoop(int nrows, std::ostream& output);
    
    // override to split map and reduce calculations over multiple threads
    virtual int multiThread(int nrows, int nthreads, std::ostream& output);
    
protected:
    bool verbose;
    int delay;
};

// ========== Function Headers =====================================================================

// copy input data from path tempInputName to hdfs:, call Hadoop streaming, read results from hdfs:
// (assumes all results are in the hdfs: file part-00000), write to output
int callHadoop(const std::string& tempInputName,
               const std::string& dirPrefix,
               bool verbose,
               std::ostream& output);

// ========== Function Templates ===================================================================

// parse input stream of the form key-tab-value-newline where key is a string and value is a string
// corresponding to the templated type
template <typename Value> bool readKeyValue(std::istream& input, std::string& key, Value& value);

template <typename Value> bool readKeyValue(std::istream& input, std::string& key, Value& value)
{
    getline(input, key, '\t');
    input >> value;
    
    // VS complains if max is not wrapped with ()
    input.ignore((std::numeric_limits<std::streamsize>::max)(), '\n');
    
    bool valid = key.length() > 0 && !input.fail();
    
    return valid;
}

// -------------------------------------------------------------------------------------------------

// write output stream of the form key-tab-value-newline where key is a string and value is a string
// corresponding to the templated type
template <typename Value> bool writeKeyValue(std::ostream& input,
                                             const std::string& key,
                                             const Value& value);

template <typename Value> bool writeKeyValue(std::ostream& output,
                                             const std::string& key,
                                             const Value& value)
{
    output << key << '\t' << value << std::endl;
    
    return !output.fail();
}


#endif
