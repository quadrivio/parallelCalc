//
//  calc.cpp
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

#include "calc.h"

#include <fstream>
#include <sstream>

#if USE_THREADS
#include <thread>
#endif

#include "callWithFork.h"
#include "utils.h"

using namespace std;

// ========== Globals ==============================================================================

#if USE_THREADS
const string gToolName = "parallelCalct";
#else

const string gToolName = "parallelCalcn";
#endif

// ========== Classes ==============================================================================

Calc::Calc() :
verbose(false),
delay(0)
{
}

Calc::~Calc()
{
}

// override to write key/value data usable as input to map operation
int Calc::startWorker(int nrows, std::ostream& output)
{
    return 0;
}

// override to read key/value starting data, write mapped data
int Calc::mapWorker(std::istream& input, std::ostream& output)
{
    return 0;
}

// override to read key/value mapped data, write reduced data
int Calc::reduceWorker(std::istream& input, std::ostream& output)
{
    return 0;
}

// call startWorker, mapWorker, reduceWorker in main thread, saving intermediate results to
// strings for debugging
int Calc::singleThreadWorkers(int nrows, std::ostream& output)
{
    int result = 0;
    
    // start
    string startStr;
    if (result == 0) {
        ostringstream oss;
        result = startWorker(nrows, oss);
        startStr = oss.str();
        
        if (verbose) {
            cerr << "Start:" << endl << startStr << endl;;
        }
    }
    
    // map
    string mappedStr;
    if (result == 0) {
        istringstream iss(startStr);
        ostringstream oss;
        result = mapWorker(iss, oss);
        mappedStr = oss.str();
        
        if (verbose) {
            cerr << "Mapped:" << endl << mappedStr << endl;;
        }
    }
    
    // reduce
    if (result == 0) {
        istringstream iss(mappedStr);
        result = reduceWorker(iss, output);
    }
    
    return result;
}

// override to handle start | map | reduce calculations directly, without writing to and
// reading from intermediate text strings
int Calc::singleThreadDirect(int nrows, std::ostream& output)
{
    return singleThreadWorkers(nrows, output);
}

// for debugging and testing: fork and call via command-line:
// parallelCalcn -start | parallelCalcn -map | parallelCalcn -reduce
// or
// parallelCalct -start | parallelCalct -map | parallelCalct -reduce
int Calc::forkWorkers(int nrows, std::ostream& output)
{
    int result = 0;
    
    string path("");
#ifdef __linux
    char *toolPathC = getenv("TOOL_PATH");
    if (toolPathC != NULL) {
        path += string(toolPathC) + "/";
    }
#endif
        
    // start
    string startStr;
    if (result == 0) {
        // arg list
        vector<string> args;
        args.push_back(gToolName);
        args.push_back("-start");
        args.push_back("-n");
        ostringstream ossArg;
        ossArg << nrows;
        args.push_back(ossArg.str());
        
        istringstream iss("");
        ostringstream oss;
        ostringstream error;
        
        result = forkPipeWait(path + gToolName, args, iss, oss, error);
        startStr = oss.str();
        
        string errorStr = error.str();
        if (verbose && errorStr.length() > 0) {
            cerr << "-start: " << errorStr << endl;;
        }
    }
    
    // map
    string mappedStr;
    if (result == 0) {
        // arg list
        vector<string> args;
        args.push_back(gToolName);
        args.push_back("-map");
        
        istringstream iss(startStr);
        ostringstream oss;
        ostringstream error;
        
        result = forkPipeWait(path + gToolName, args, iss, oss, error);
        mappedStr = oss.str();
        
        string errorStr = error.str();
        if (verbose && errorStr.length() > 0) {
            cerr << "-map:" << endl << errorStr << endl;;
        }
    }
    
    // reduce
    string reducedStr;
    if (result == 0) {
        // arg list
        vector<string> args;
        args.push_back(gToolName);
        args.push_back("-reduce");
        
        istringstream iss(mappedStr);
        ostringstream oss;
        ostringstream error;
        
        result = forkPipeWait(path + gToolName, args, iss, output, error);
        
        string errorStr = error.str();
        if (verbose && errorStr.length() > 0) {
            cerr << "-reduce:" << endl << errorStr << endl;;
        }
    }
    
    return result;
}

// call parallelCalc -map and parallelCalc -reduce via Hadoop streaming
int Calc::hadoop(int nrows, std::ostream& output)
{
    // write temp local data file
    string tempInputName = tmpnam(NULL);
    ofstream ofs(tempInputName.c_str());
    
    // write starting data to file
    startWorker(nrows, ofs);
    ofs.close();
    
    callHadoop(tempInputName, name(), verbose, output);
    
    // delete temp file
    remove(tempInputName.c_str());    
    
    return 0;
}

// override to split map and reduce calculations over multiple threads
int Calc::multiThread(int nrows, int nthreads, std::ostream& output)
{
    return 0;
}

// ========== Functions ============================================================================

// copy input data from path tempInputName to hdfs:, call Hadoop streaming, read results from hdfs:
// (assumes all results are in the hdfs: file part-00000), write to output
int callHadoop(const std::string& tempInputName,
               const std::string& dirPrefix,
               bool verbose,
               std::ostream& output)
{
    char *hadoopInstallC = getenv("HADOOP_INSTALL");
    
    RUNTIME_ERROR_IF(hadoopInstallC == NULL, "undefined environment variable HADOOP_INSTALL");
    
    string hadoopInstall(hadoopInstallC);    
    string hadoopPath = hadoopInstall + "/bin/hadoop";
    
    string stdoutStr;
    string stderrStr;
    
    // make input directory
    callTool("hadoop", hadoopPath, stdoutStr, stderrStr, verbose,
             "dfs", "-mkdir", (dirPrefix + "Input").c_str(), NULL);
    
    // remove previous input file (if any)
    callTool("hadoop", hadoopPath, stdoutStr, stderrStr, verbose,
             "dfs", "-rm", (dirPrefix + "Input/input.txt").c_str(), NULL);
    
    // write new input file
    callTool("hadoop", hadoopPath, stdoutStr, stderrStr, verbose,
             "dfs", "-put", tempInputName.c_str(), (dirPrefix + "Input/input.txt").c_str(), NULL);
    
    // remove previous hdfs output directory (if any)
    callTool("hadoop", hadoopPath, stdoutStr, stderrStr, verbose,
             "dfs", "-rmr", (dirPrefix + "Output").c_str(), NULL);
    
    // run hadoop calculation
    string jarPath = hadoopInstall + "/contrib/streaming/hadoop-streaming-1.1.2.jar";
    
    string quote("\"");

    string toolPath = getWorkingDirectory();
#ifdef __linux
    char *toolPathC = getenv("TOOL_PATH");
    if (toolPathC != NULL) {
        toolPath += string("/") + toolPathC;
    }
#endif
    
    toolPath.append("/");
    toolPath.append(gToolName);

    string mapperCmd = quote + gToolName + " -map" + quote;
    string reducerCmd = quote + gToolName + " -reduce" + quote;
    
    callTool("hadoop", hadoopPath, stdoutStr, stderrStr, verbose,
             "jar",
             jarPath.c_str(),
             "-input",
             (dirPrefix + "Input").c_str(),
             "-output",
             (dirPrefix + "Output").c_str(),
             "-mapper",
             mapperCmd.c_str(),
             "-reducer",
             reducerCmd.c_str(),
             "-file",
             toolPath.c_str(),
             NULL);

    // copy results to local file
    string tempOutputName = tmpnam(NULL);
    callTool("hadoop", hadoopPath, stdoutStr, stderrStr, verbose,
             "dfs", "-get", (dirPrefix + "Output/part-00000").c_str(), tempOutputName.c_str(),
             NULL);
    
    // copy results file to output
    ifstream ifs(tempOutputName.c_str());
    output << ifs.rdbuf();
    ifs.close();
    
    return 0;
}
