CXX = g++
RM = rm -rf
CXXFLAGS = -Wall -Wextra -Werror -Woverloaded-virtual -Wpointer-arith -Wcast-qual \
           -Wwrite-strings -Winit-self -Wno-sign-compare -Wno-unused-parameter \
           -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK -fno-omit-frame-pointer \
           -fvisibility=default -DBOOST_SP_DISABLE_THREADS -DBOOST_DISABLE_THREADS \
           -DBOOST_ALL_NO_LIB -Wno-unused-local-typedefs -Wno-absolute-value \
           -Wno-ignored-qualifiers -fno-strict-aliasing --std=c++11 -g3 -O3 -mmmx -msse \
           -msse2 -msse3 -DNDEBUG -DVOLT_LOG_LEVEL=500 -DMACOSX -arch x86_64 -MMD -MP \
           -fPIC -shared -DUDF_COMPILE
ISYSTEM = -isystem /Users/yzhang/Github/voltdb/third_party/cpp \
		  -isystem /Users/yzhang/Github/voltdb/third_party/cpp/google-s2-geometry/geometry/include
INCLUDE = -I/Users/yzhang/Github/voltdb/src/ee -I.

all: libtrigonometry.so

libtrigonometry.so: trigonometry.cpp
	$(CXX) $(CXXFLAGS) $(ISYSTEM) $(INCLUDE) -o $@ $^

clean:
	$(RM) *.so *.so.* *.d
