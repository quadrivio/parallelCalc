//
//  main.cpp
//  parallelCalc
//
//  Created by MPB on 7/4/13.
//  Copyright (c) 2013 Quadrivio Corporation. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted
// provided that the following conditions are met:
// 
// Redistributions of source code must retain the above copyright notice, this list of conditions
// and the following disclaimer.
//
// Redistributions in binary form must reproduce the above copyright notice, this list of conditions
// and the following disclaimer in the documentation and/or other materials provided with the
// distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
// WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

//
// Interpret command-line arguments
//

#include "shim.h"

#if WINDOWS
#include <stdio.h>
#include <tchar.h>
#include <Windows.h>
#endif

#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "sumSquare.h"
#include "utils.h"

using namespace std;

#define USE_HADOOP !WINDOWS

void usage();

int main (int argc, const char * argv[])
{
#if 0
    cerr << "USE_THREADS = " << USE_THREADS << endl << endl;
    for (int k = 0; k < argc; k++) {
        if (k > 0) cerr << " ";
        cerr << argv[k];
    }
    cerr << endl << endl;
#endif
    
    Calc *calc = new SumSquare();
    
    // Arguments and (Flags):
    //
    //  -n          number of rows to calculate
    //  -d          additional delay per map calculation in milliseconds
    //
    //  -start      send input rows to stdout
    //  -map        read rows from stdin, write mapped rows to stdout
    //  -reduce     read mapped rows from stdin, write reduced rows to stdout
    //
    //  -threads    number of threads to use with multithreading
    //  -hadoop     use hadoop
    //  -fork       test fork
    //
    //  -v          verbose
    
    int status = 1;
    
    try {
        bool paramError = false;
        bool printUsage = argc <= 1;
        
        int nrows = 10;
        int delay = 0;
        bool startFlag = false;
        bool mapFlag = false;
        bool reduceFlag = false;
        bool threadsFlag = false;
        int nthreads = 1;
        bool hadoopFlag = false;
        bool forkFlag = false;
        
        for (int index = 1; index < argc; index++) {
            if (strcmp(argv[index], "-n") == 0) {
                nrows = atoi(argv[++index]);
                if (nrows <= 0 || nrows > 1000) {
                    paramError = true;
                    cerr << "-n value must be > 0 and <= 1000" << endl;
                }
                
            } else if (strcmp(argv[index], "-d") == 0) {
                delay = atoi(argv[++index]);
                if (delay < 0 || delay > 60000) {
                    paramError = true;
                    cerr << "-d value must be >= 0 and <= 60000" << endl;
                    
                } else {
                    calc->setDelay(delay);
                }
                
            } else if (strcmp(argv[index], "-start") == 0) {
                startFlag = true; 
                
            } else if (strcmp(argv[index], "-map") == 0) {
                mapFlag = true;
                
            } else if (strcmp(argv[index], "-reduce") == 0) {
                reduceFlag = true;

#if USE_HADOOP
            } else if (strcmp(argv[index], "-hadoop") == 0) {
                hadoopFlag = true;
#endif
                
            } else if (strcmp(argv[index], "-fork") == 0) {
                forkFlag = true;
                
#if USE_THREADS
            } else if (strcmp(argv[index], "-threads") == 0) {
                nthreads = atoi(argv[++index]);
                if (nthreads < 0 || nthreads > 64) {
                    paramError = true;
                    cerr << "-threads value must be >= 0 and <= 64" << endl;
                    
                } else {
                    threadsFlag = true;
                }
#endif
                
            } else if (strcmp(argv[index], "-v") == 0) {
                calc->setVerbose(true);
                
            } else {
                printUsage = true;
            }
        }
        
        int atMostOne = 0;
        
        if (startFlag) atMostOne++;
        if (mapFlag) atMostOne++;
        if (reduceFlag) atMostOne++;
        if (threadsFlag) atMostOne++;
        if (hadoopFlag) atMostOne++;
        if (forkFlag) atMostOne++;
        
        if (atMostOne > 1) {
            paramError = true;
            cerr << "use at most one of -start -map -reduce -hadoop -threads -fork" << endl;
        }
        
        if (paramError) {
            // skip
            
        } else if (printUsage) {
            usage();
            
        } else if (atMostOne == 0) {
            // default - single-threaded calls to worker methods
            long long startTime = millisecondTime();
            
            status = calc->singleThreadWorkers(nrows, cout);

            long long endTime = millisecondTime();
            cerr << (status ? "FAILURE " : "OK ");
            cerr << fixed << setprecision(3) << 0.001 * (endTime - startTime) << " seconds" << endl;

        } else if (forkFlag) {
            long long startTime = millisecondTime();
            
            status = calc->forkWorkers(nrows, cout);
            
            long long endTime = millisecondTime();
            cerr << (status ? "FAILURE " : "OK ");
            cerr << fixed << setprecision(3) << 0.001 * (endTime - startTime) << " seconds" << endl;
            
        } else if (hadoopFlag) {
            long long startTime = millisecondTime();
            
            status = calc->hadoop(nrows, cout);
            
            long long endTime = millisecondTime();
            cerr << (status ? "FAILURE " : "OK ");
            cerr << fixed << setprecision(3) << 0.001 * (endTime - startTime) << " seconds" << endl;
            
        } else if (threadsFlag) {
            long long startTime = millisecondTime();
            
            if (nthreads == 0) {
                // for testing direct access methods
                status = calc->singleThreadDirect(nrows, cout);
                
            } else {
                status = calc->multiThread(nrows, nthreads, cout);
            }
            
            long long endTime = millisecondTime();
            cerr << (status ? "FAILURE " : "OK ");
            cerr << fixed << setprecision(3) << 0.001 * (endTime - startTime) << " seconds" << endl;
            
        } else if (startFlag) {
            status = calc->startWorker(nrows, cout);
            
        } else if (mapFlag) {
            status = calc->mapWorker(cin, cout);
            
        } else if (reduceFlag) {
            status = calc->reduceWorker(cin, cout);
        }
        
        status = 0;
        
    } catch (const logic_error& x) {
        std::cerr << "logic_error: " << x.what() << endl;
        
    } catch (const runtime_error& x) {
        std::cerr << "runtime_error: " << x.what() << endl;
        
    } catch (...) {
        std::cerr << "unknown error" << endl;
    }
    
    return status;
}

void usage()
{
    cerr << "usage: parallelCalc [-n <nrows>] [-d <delay>] [-start | -map | -reduce";
    
#if USE_THREADS
    cerr << " | -threads <nthreads>";
#endif
    
#if USE_HADOOP
    cerr << " | -hadoop";
#endif

    cerr << "]" << endl;
    
    cerr <<
    "  -n       number of rows to calculate" << endl <<
    "  -d       additional delay per map calculation in milliseconds" << endl <<
    "  -start   send input rows to stdout" << endl <<
    "  -map     read rows from stdin, write mapped rows to stdout" << endl <<
    "  -reduce  read mapped rows from stdin, write reduced rows to stdout" << endl;
    
#if USE_THREADS
    cerr << "  -threads number of threads to use with multithreading" << endl;
#endif

#if USE_HADOOP
    cerr << "  -hadoop  use hadoop" << endl;
#endif
    
    cerr << "  -fork    call command-line tools" << endl;
    
    cerr << "  -v       verbose" << endl;
}
