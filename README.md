# Parallel Sort Code in C++

The code provided in this repository implements a parallel sort algorithm that follows the same conventions as std::sort().  It works well on large data sets, and performance improves with any number of threads up to the point where memory bandwidth is the limit.  It works with arrays and std::vectors.  The code has been compiled with MSVC, clang and g++.


## Description

The two functions provided are

```cpp

  template<class RandomIt>
  void parallelSort(RandomIt begin, RandomIt end, size_t threads = 0) 
  template<class RandomIt, class CF>
  void parallelSort(RandomIt begin, RandomIt end, CF compFunc , size_t threads = 0)
  
```

where

- **begin** is an iterator to the start of the data to be sorted.
- **end** is an iterator to the end of the data to be sorted.
- **threads** is an optional parameter to indicate the number of threads to use.  The default is set to the hardware_concurrency().
- **compFunc** is an optional pointer to a function that performs the sorting comparison.  The default is std::less

In most cases, it is possible to just replace std::sort with parrallelSort to get improves sorting performance.

## Example Code
ParallelSortTest.cpp contains the option to to compile the code below to show that it is compatible with std::sort().
```
// This code was taken from cppreferences website sort example code at https://en.cppreference.com/w/cpp/algorithm/sort
//  The only changes are that std::sort() was replaced with parallelSort.
#include <algorithm>
#include <array>
#include <functional>
#include <iostream>
#include <string_view>
#include "parallelSort.hpp"
int main()
{
  std::array&lt;int, 10> s{ 5, 7, 4, 2, 8, 6, 1, 9, 0, 3 };
  auto print = [&s](std::string_view const rem)
  {
    for (auto a : s)
      std::cout &lt;< a &lt;< ' ';
    std::cout &lt;< ": " &lt;< rem &lt;< '\n';
  };
  parallelSort(s.begin(), s.end());
  print("sorted with the default operator&lt;");
  parallelSort(s.begin(), s.end(), std::greater&lt;int>());
  print("sorted with the standard library compare function object");
  struct
  {
    bool operator()(int a, int b) const { return a &lt; b; }
  }
  customLess;
  parallelSort(s.begin(), s.end(), customLess);
  print("sorted with a custom function object");
  parallelSort(s.begin(), s.end(), [](int a, int b)
    {
      return a > b;
    });
  print("sorted with a lambda expression");
}

```


## Algorithm

My exploration of parallel sorting can be found at [https://github.com/johnarobinson77/Explorations-of-Parallel-Merge-Sort](https://github.com/johnarobinson77/Explorations-of-Parallel-Merge-Sort).  But here is a brief explanation.

parallelSort breaks up the input data into a numer of segments equal to the number of threads.  It then sorts each of those segments using std::sort on separate threads, resulting in each of the segments being sorted.

After that, the segments are merged together using a parallel merge algorithm originally developed for GPU sorting by Greenand et al [1].  Each merge uses the total number of specified threads.  The segments are iteratively merged into larger and larger segmens until these is only one segment in the original structure.


## Performance

The following performance bar graphs show perfprmance with threads from 1 to 20 on a 48 core Graviton3 on AWS.  The test size is 16,777,216 elements.  Note that the performance increases with each increase in core count.  After that the sort is limited by memory bandwidth.

![Integer Sort Performance](https://github.com/johnarobinson77/ParallelSort/blob/main/Integers.png)
![String Sort Performance](https://github.com/johnarobinson77/ParallelSort/blob/main/Strings.png)

## References

[1] Greenand, McColl, and Bader.  GPU Merge Path: A GPU Merging Algorithm

     Proceedings of the 26th ACM International Conference on Supercomputing
