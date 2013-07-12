//
//  callWithFork.cpp
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

#include "callWithFork.h"

#if !WINDOWS
#include <unistd.h>
#include <sys/wait.h>
#endif

#include <cstdarg>
#include <sstream>

#include "utils.h"

using namespace std;

// ========== Functions ============================================================================

// fork process, call command-line tool with specified arguments, pipe stdin, stdout, sterr, wait
// for completion; returns 0 for success; assumes moderate amount of input and output
int forkPipeWait(const std::string& path, std::vector<std::string> args, std::istream& input,
                  std::ostream& output, std::ostream& error)
{
    int result = 1;
    
#if WINDOWS
	LOGIC_ERROR_IF(true, "forkPipeWait: not implemented");

#else
    const int READ_END = 0;
    const int WRITE_END = 1;
    
    int childInputPipe[2];
    int childOutputPipe[2];
    int childErrorPipe[2];
    
    bool childInputPipeOK = pipe(childInputPipe) == 0;
    bool childOutputPipeOK = pipe(childOutputPipe) == 0;
    bool childErrorPipeOK = pipe(childErrorPipe) == 0;
    
    if (childInputPipeOK && childOutputPipeOK && childErrorPipeOK) {
        // pipes OK
        
        pid_t pid = fork();
        
        if (pid == 0) {
            // in child process
            
            // close unused ends
            close(childInputPipe[WRITE_END]);
            close(childOutputPipe[READ_END]);
            close(childErrorPipe[READ_END]);
            
            fflush(stdout);
            fflush(stderr);
            
            bool inDupOK = dup2(childInputPipe[READ_END], STDIN_FILENO) >= 0;
            bool outDupOK = dup2(childOutputPipe[WRITE_END], STDOUT_FILENO) >= 0;
            bool errDupOK = dup2(childErrorPipe[WRITE_END], STDERR_FILENO) >= 0;
            
            if (inDupOK && outDupOK&& errDupOK) {
                vector<const char *> argv;
                for (size_t k = 0; k < args.size(); k++) {
                    argv.push_back(args[k].c_str());
                }
                
                argv.push_back(NULL);
                
                if (path.length() == 0) {
                    // use search PATH
                    execvp(argv[0], (char * const *)&argv[0]);
                    
                } else {
                    // explicit path
                    execv(path.c_str(), (char * const *)&argv[0]);
                }
            }
            
            _exit(EXIT_FAILURE);
            
        } else if (pid < 0) {
            error << "fork failed";
            
        } else {
            // in parent process
            
            // close unused ends
            close(childInputPipe[READ_END]);
            close(childOutputPipe[WRITE_END]);
            close(childErrorPipe[WRITE_END]);
            
            const int BUFSIZE = 4096;
            char buffer[BUFSIZE];
            ssize_t nbytes = BUFSIZE;
            
            // the following assumes it is possible to completely fill input pipe before collecting
            // anything from output and error pipes.
            // TODO - interleave reading and writing to handle cases when pipe buffers fill & block
            
            // fill input pipe
            do {
                input.read(buffer, BUFSIZE);
                if (input.eof()) {
                    nbytes = input.gcount();                    
                }
                
                write(childInputPipe[WRITE_END], buffer, (size_t)nbytes);
                
            } while (nbytes == BUFSIZE);
            
            close(childInputPipe[WRITE_END]);
            
            // collect from output pipe
            do {
                nbytes = read(childOutputPipe[READ_END], buffer, BUFSIZE - 1);
                
                if (nbytes > 0) {
                    buffer[nbytes] = '\0';
                    output << buffer;
                }
                
            } while (nbytes > 0);
            
            // collect from error pipe
            do {
                nbytes = read(childErrorPipe[READ_END], buffer, BUFSIZE - 1);
                
                if (nbytes > 0) {
                    buffer[nbytes] = '\0';
                    error << buffer;
                }
                
            } while (nbytes > 0);
            
            // done
            int status;
            waitpid(pid, &status, 0);
            
            if (WIFEXITED(status)) {
                if (WEXITSTATUS(status) == 0) {
                    result = 0;
                    
                } else {
                    error << "  WEXITSTATUS = " << WEXITSTATUS(status);
                }
                
            } else if (WIFSIGNALED(status)) {
                error << "  WTERMSIG = " << WTERMSIG(status);
                
            } else if (WCOREDUMP(status)) {
                error << "  WCOREDUMP";
                
            } else if (WIFSTOPPED(status)) {
                error << "  WSTOPSIG = " << WSTOPSIG(status);
            }
            
            close(childOutputPipe[READ_END]);
            close(childErrorPipe[READ_END]);
        }
    }
#endif
    
    return result;
}

// convenience function: call forkPipeWait with empty input and variable number of arguments, return
// stdout and sterr results as strings
int callTool(const std::string& toolName, const std::string& toolPath, std::string& stdoutStr, 
              std::string& stderrStr, bool logging, const char *arg, ...)
{
    int result = 1;
    
#if WINDOWS
	LOGIC_ERROR_IF(true, "forkPipeWait: not implemented");

#else
    
    // collect variable number of arguments, using standard magic; store in vector
    vector<string> args;

    va_list vargs;
    va_start(vargs, arg);
    
    args.push_back(toolName);
    
    const char *next = arg;
    while (next != NULL) {
        args.push_back(next);
        next = va_arg(vargs, const char *);
    }
    
    va_end(vargs);
    
    if (logging) {
        // print command line
        ostringstream oss;
        for (size_t k = 0; k < args.size(); k++) {
            if (k > 0) oss << " ";
            oss << args[k];
        }
        string command = oss.str();
        
        cerr << endl << "'" << command << "'" << endl;
    }
    
    istringstream input("");
    ostringstream output;
    ostringstream error;
    
    result = forkPipeWait(toolPath, args, input, output, error);
    
    stdoutStr = output.str();
    stderrStr = error.str();
    
    if (logging) {
        if (stdoutStr.length() > 0) {
            cerr << "  [stdout] " << stdoutStr << endl;
        }
        
        if (stderrStr.length() > 0) {
            cerr << "  [stderr] " << stderrStr << endl;
        }
    }
#endif 
    
    return result;
}

