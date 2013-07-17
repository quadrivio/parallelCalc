//
//  shim.h
//  parallelCalc
//
//  Created by MPB on 7/4/13.
//  Copyright (c) 2013 Quadrivio Corporation. All rights reserved.
//

#ifndef parallelCalc_shim_h
#define parallelCalc_shim_h

#define WINDOWS (defined _WIN32 || defined _WIN64)

#if WINDOWS
// use single-width characters
#undef UNICODE
#undef _UNICODE
#endif

// need to include a library so that flags used below can be checked
#include <iostream>

// ========== COMPILE FLAGS ========================================================================

#ifndef USE_THREADS

#if __cplusplus >= 201103L && defined(_LIBCPP_VERSION)  // XCode on Mac OS X 10.7
#define USE_THREADS 1

#elif defined(_MSC_VER) && _MSC_VER >= 1700     // Visual Studio Express 2012 on Windows 7
#define USE_THREADS 1

#elif defined(__linux) && defined(__GXX_EXPERIMENTAL_CXX0X__)   // Eclipse Kepler on CentOS 6.4
#define USE_THREADS 1

#else
#define USE_THREADS 0
#endif

#endif

#endif
