// ParallelSort.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

/**
* Copyright(c) 2023 John Robinson.
*
*SPDX - License - Identifier: BSD - 3 - Clause
*/

//#define  LIB_EXAMPLE_CODE
#ifdef LIB_EXAMPLE_CODE
// This code was taken from cppreferences website sort example code at https://en.cppreference.com/w/cpp/algorithm/sort
// The only changes are that std::sort() was replaced with parallelSort.
#include <algorithm>
#include <array>
#include <functional>
#include <iostream>
#include <string_view>
#include "parallelSort.hpp"

int main()
{
  std::array<int, 10> s{ 5, 7, 4, 2, 8, 6, 1, 9, 0, 3 };

  auto print = [&s](std::string_view const rem)
  {
    for (auto a : s)
      std::cout << a << ' ';
    std::cout << ": " << rem << '\n';
  };

  parallelSort(s.begin(), s.end());
  print("sorted with the default operator<");

  parallelSort(s.begin(), s.end(), std::greater<int>());
  print("sorted with the standard library compare function object");

  struct
  {
    bool operator()(int a, int b) const { return a < b; }
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

#else

#include <iostream>
#include <random>
#include <chrono>
#include <vector>
#include <list>
#include <cstring>
#include <iomanip>
#include <mutex>
#include "parallelFor.hpp"
#include "parallelSort.hpp"

// a slight rewrite of the Romdomer class from
// https://stackoverflow.com/questions/13445688/how-to-generate-a-random-number-in-c/53887645#53887645
// Usage: int64_t example
// auto riTestData = RandomInterval<int64_t>(-10000000000LL, 10000000000LL, 1);
// int64_t ri = riTestData();

template <typename RDT>
class RandomInterval {
  // random seed by default
  std::mt19937 gen_;
  std::uniform_int_distribution<RDT> dist_;

public:
  RandomInterval(RDT min, RDT max, unsigned int seed = std::random_device{}())
    : gen_{ seed }, dist_{ min, max } {}

  // if you want predictable numbers
  void SetSeed(unsigned int seed) { gen_.seed(seed); }

  RDT operator()() { return dist_(gen_); }
};


// Verification function used when data is directly sorted.
template< class RandomIt>
bool sortVerifier(RandomIt test_data, RandomIt reference, const size_t test_size, size_t threads = 0) {
  // default number of threads iw the hardware number of cores.
  if (threads == 0) threads = std::thread::hardware_concurrency();
  // Get the type;
  typedef typename std::iterator_traits<RandomIt>::value_type T;

  // run the verify function in parallel because you can.  Needs and atomic error counter and checksum register.
  std::atomic<size_t> errCnt{ 0 };

  // The verify portion makes sure the data is sorted correctly by checking that each array element is >= the one before it.
  // It also computes a checksum which is compared against the source data's checksum to check for data corruption.
  parallelFor((size_t)1, test_size, [&errCnt, test_data, reference](size_t lc) {
    static std::mutex lock;
    std::lock_guard<std::mutex> guard(lock);
      if (*(test_data + lc) != *(reference + lc)) {
        if (errCnt == 0) {
          std::cout << "First error at " << lc << std::endl;
          std::cout << "[" << lc << "] :" << *(test_data + lc) << " != " << *(reference + lc) << std::endl;
        }
        errCnt++;
      }
    }, (size_t)8);
  bool thisTestFailed = false;
  if (errCnt > 0) {
    std::cout << "Total of " << errCnt << " errors out of " << test_size << std::endl;
    thisTestFailed = true;
  }

  return thisTestFailed;
}

// Verification function used when pointers to data are sorted.
template< class RandomIt>
bool sortVerifierP(RandomIt test_data, RandomIt reference, const size_t test_size, size_t threads = 0) {
  // default number of threads is the hardware number of cores.
  if (threads == 0) threads = std::thread::hardware_concurrency();
  // Get the type;
  typedef typename std::iterator_traits<RandomIt>::value_type T;

  // run the verify function in parallel because you can.  Needs and atomic error counter and checksum register.
  std::atomic<size_t> errCnt{ 0 };

  // The verify portion makes sure the data is sorted correctly by checking that each array element is >= the one before it.
  // It also computes a checksum which is compared against the source data's checksum to check for data corruption.
  parallelFor((size_t)1, test_size, [&errCnt, test_data, reference](size_t lc) {
    static std::mutex lock;
    std::lock_guard<std::mutex> guard(lock);
    if (**(test_data + lc) != **(reference + lc)) {
      if (errCnt == 0) {
        std::cout << "First error at " << lc << std::endl;
        std::cout << "[" << lc << "] :" << **(test_data + lc) << " != " << **(reference + lc) << std::endl;
      }
      errCnt++;
    }
    }, (size_t)8);
  bool thisTestFailed = false;
  if (errCnt > 0) {
    std::cout << "Total of " << errCnt << " errors out of " << test_size << std::endl;
    thisTestFailed = true;
  }

  return thisTestFailed;
}


// names for the available test data types
const int64_t dtRandom = 0;
const int64_t dtOrdered = 1;
const int64_t dtReverseOrdered = 2;

// abstract class for the test cases
class SortCase {
public:
  SortCase() {}

  // This function generates the test data.  Called once for fixed lenth tests 
  //and every loop for random size tests
  virtual void generateData(size_t test_size,size_t  data_type) = 0;

  // This function copies the sours data to a test structure to be sorted
  // by parallel sort.  It returns the time to do the parallel sort in seconds
  virtual double runSort(size_t test_size, size_t threads) = 0;

  // If the verify option is selected, this function is called insure the data was
  // sorted correctly.
  virtual bool verifySort(size_t test_size) = 0;

  // This function just deletes the the source data and test data structures
  virtual void cleanup() = 0;

};

// This is the test case for sort test #3.  A set of random 5 character strings are
// generated and stored in std::vector strings.  For the test, std::vector stringPtrs 
// is filled with pointers to the generated strings. stringPtrs is submitted to parallelSort
// with a comparator function that compares the strings are pointed to.  The pointers are 
// moved in memory not the strings.  
//  For verification, the same is done to the reference vector but sorted using the std::sort.  
// Then the two are compared and any difference is logged as a failure
class textPointerSortCase : SortCase {

  RandomInterval<short>  rs = RandomInterval<short>('0', '}', 1);
  // create test_size random strings.
  std::vector<std::string>* strings = nullptr;
  std::vector<std::string*>* stringPtrs = nullptr;
  std::vector<std::string*>* reference = nullptr;

  struct
  {
    bool operator()(std::string* a, std::string* b) const { return *a > *b; }
  }
  stringLess;

public:
  textPointerSortCase() {}

  void generateData(size_t test_size, size_t data_type) {
    // create test_size random strings.
    if (strings != nullptr) delete strings;
    strings = new std::vector<std::string>(test_size);
    if (stringPtrs != nullptr) delete stringPtrs;
    stringPtrs = new std::vector<std::string*>(test_size);

    // generate a 5 character c style string and write the string 
    // to the strings vector
    for (size_t i = 0; i < test_size; i++) {
      char chr[6];
      for (int k = 0; k < 5; k++) chr[k] = (char)rs();
      chr[5] = 0;
      strings->at(i) = std::string(chr);
    }
  }

  double runSort(size_t test_size, size_t threads) {

    // get the pointers to the strings in the strings vector.
    for (size_t i = 0; i < test_size; i++) {
      stringPtrs->at(i) = &(strings->at(i));
    }

    // Get starting timepoint
    auto start = std::chrono::high_resolution_clock::now();
    // call the sort case
    parallelSort(stringPtrs->begin(), stringPtrs->end(), stringLess, threads);
    auto stop = std::chrono::high_resolution_clock::now();

    // calculate and return the execution time.
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    return (duration.count() / 1000000.0);
  }

  bool verifySort(size_t test_size) {

    // get the pointers to the strings in the strings vector
    reference = new std::vector<std::string*>(test_size);

    for (int i = 0; i < test_size; i++) {
      reference->at(i) = &(strings->at(i));
    }
    std::sort(reference->begin(), reference->end(), stringLess);
    bool thisTestFailed;
    thisTestFailed = sortVerifierP(stringPtrs->begin(), reference->begin(), test_size);
    delete reference;
    return thisTestFailed;
  }

  void cleanup() {
    delete strings;
    delete stringPtrs;
    strings = nullptr;
    stringPtrs = nullptr;
  }

};

// This is the test case for sort test #1.  The data is generated in source_data[].
// For the test, the data is copied from source_data to int64_t test_data[]
// and sorted with parallel_sort using the default sort direction.  For verification, 
// the same is done to the reference[] array but sorted using the std::sort.  Then the two 
// are compared and any difference is logged as a failure
class arraySortCase : SortCase {

  RandomInterval<int64_t> riTestData = RandomInterval<int64_t>(-10000000000LL, 10000000000LL, 1);
  // create test_size random strings.
  int64_t* source_data = nullptr;
  int64_t* test_data = nullptr;
  int64_t* reference = nullptr;

public:
  arraySortCase() {
  }

  void generateData(size_t test_size, size_t data_type) {

    if (source_data != nullptr) delete[] source_data;
    source_data = new int64_t[test_size];
    if (test_data != nullptr) delete[] test_data;
    test_data = new  int64_t[test_size];

    // create the requested data type.
    switch (data_type) {
    case dtRandom: { // generate random data
      for (size_t i = 0; i < test_size; i++) source_data[i] = riTestData();
      // make about 5% of the data equal at prime number spacings
      for (size_t i = 0; i < test_size; i += 19) source_data[i] = source_data[test_size - i];
      break;
    }
    case dtOrdered: {  // generate ordered data
      for (size_t i = 0; i < test_size; i++) source_data[i] = (i);
      break;
    }
    case dtReverseOrdered: { // generate reverse ordered data
      for (size_t i = 0; i < test_size; i++) source_data[i] = (test_size - i);
      break;
    }
    default: {
      std::cout << "No such data type: " << data_type << std::endl;
      exit(1);
    }
    }
  }

  double runSort(size_t test_size, size_t threads) {

    // get data to sort from source data
      // create the array to be sorted and copy the source data to it.
    memcpy(test_data, source_data, test_size * sizeof(int64_t));


    // Get starting timepoint
    auto start = std::chrono::high_resolution_clock::now();
    // call the sort case
    parallelSort(test_data, test_data + test_size, threads);
    auto stop = std::chrono::high_resolution_clock::now();

    // calculate and return the execution time.
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    return (duration.count() / 1000000.0);
  }

  bool verifySort(size_t test_size) {

    // generate the reference data
    reference = new int64_t[test_size];
    for (int i = 0; i < test_size; i++) {
      reference[i] = source_data[i];
    }

    std::sort(reference, reference + test_size);
    bool thisTestFailed = sortVerifier
      (test_data, reference, test_size);
    delete reference;
    return thisTestFailed;
  }

  void cleanup() {
    delete source_data;
    delete test_data;
    source_data = nullptr;
    test_data = nullptr;
  }

};


// This is the test case for sort test #2.  The data is generated in source_data[].
// FOr the test, the data is copied from source_data to std::vector<int64_t> test_data
// and sorted with parallel_sort using the std::greater.  For verification, the same is done 
// the reference vector but sorted using the std::sort.  Then the two are compared and
// any difference is logged as a failure

class vectorSortCase : SortCase {

  RandomInterval<int64_t> riTestData = RandomInterval<int64_t>(-10000000000LL, 10000000000LL, 1);
  // create test_size random strings.
  int64_t* source_data = nullptr;
  std::vector<int64_t>* test_data = nullptr;
  std::vector<int64_t>* reference = nullptr;

public:
  vectorSortCase() {}

  void generateData(size_t test_size, size_t data_type) {

    if (source_data != nullptr) delete[] source_data;
    source_data = new int64_t[test_size];
    if (test_data != nullptr) delete test_data;
    test_data = new std::vector<int64_t>(test_size);

    // create the requested data type.
    switch (data_type) {
    case dtRandom: { // generate random data
      for (size_t i = 0; i < test_size; i++) source_data[i] = riTestData();
      // make about 5% of the data equal at prime number spacings
      for (size_t i = 0; i < test_size; i += 19) source_data[i] = source_data[test_size - i];
      break;
    }
    case dtOrdered: {  // generate ordered data
      for (size_t i = 0; i < test_size; i++) source_data[i] = (i);
      break;
    }
    case dtReverseOrdered: { // generate reverse ordered data
      for (size_t i = 0; i < test_size; i++) source_data[i] = (test_size - i);
      break;
    }
    default: {
      std::cout << "No such data type: " << data_type << std::endl;
      exit(1);
    }
    }
  }

  double runSort(size_t test_size, size_t threads) {

    // create the array to be sorted and copy the source data to it.
    for (size_t i = 0; i < test_size; i++) test_data->at(i) = source_data[i];


    // Get starting timepoint
    auto start = std::chrono::high_resolution_clock::now();
    // call the sort case
    parallelSort(test_data->begin(), test_data->end(), std::greater<int64_t>(), threads);
    auto stop = std::chrono::high_resolution_clock::now();

    // calculate and return the execution time.
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    return (duration.count() / 1000000.0);
  }

  bool verifySort(size_t test_size) {

    // generate the reference data
    reference = new std::vector<int64_t>(test_size);

    for (int i = 0; i < test_size; i++) {
      reference->at(i) = source_data[i];
    }
    std::sort(reference->begin(), reference->end(), std::greater<int64_t>());
    bool thisTestFailed = sortVerifier(test_data->begin(), reference->begin(), test_size);
    delete reference;
    return thisTestFailed;
  }

  void cleanup() {
    if (source_data != nullptr) delete source_data;
    if (test_data != nullptr) delete test_data;
    source_data = nullptr;
    test_data = nullptr;
  }

};


// documentation of program arguments;
void printHelp() {
  std::cout << "Usage:\n";
  std::cout << "ParallelSortTest [-t <test number>] [-n <test_size> | -rs] [-minT <min threads>] [-maxT <max threads>] [-l <num tests per thread>] [-dr | -do | db] [-v | -nv]\n";
  std::cout << "  -t <test number> indicates test to run\n";
  std::cout << "     1 = sort array integers, 2 = sort std::vector of integers, 3 = sort vector of pointers to strings.  Default = 1\n";
  std::cout << "  -n <test size>: number of elements to sort on each test loop.\n";
  std::cout << "  -rs: randomize the test size.  Default \n";
  std::cout << "  -minT <min Threads>\n";
  std::cout << "  -maxT <max Threads> minT and maxT set the minimum and maximum threads the program will loop over.  Defaults are 1 and 8 \n";
  std::cout << "  -l <num tests per thread> sets tnu number of tests that will be run for each thread\n";
  std::cout << "  -dr | do | -db set the type of data for each test; random or ordered or reverse ordered respectively.  Default is -dr\n";
  std::cout << "  -v or -nv indicate whether to verify the sort  Default is -v\n";
}


int main(int argc, char* argv[]) {

  // default settings
  bool fixedTestSize = false;
  size_t fixedTestSizeNum = 1024 * 1024 * 16;
  size_t minThreads = 1;
  size_t maxThreads = 8;
  bool verifySort = true;
  size_t sortTestSel = 1;
  size_t num_tests = 25;
  size_t dataType = dtRandom;

  // parse the program arguments
  bool argError = false;
  for (size_t arg = 1; arg < argc; arg++) {
    bool oneMore = arg < (argc - 1);
    if (strcmp(argv[arg], "-minT") == 0) {

      arg++;
      if (!oneMore || 0 == (minThreads = atoi(argv[arg]))) {
        std::cout << "-minT requires a non-zero integer argument." << std::endl;
        argError = true;
      }
    }
    else if (strcmp(argv[arg], "-maxT") == 0) {
      arg++;
      if (!oneMore || 0 == (maxThreads = atoi(argv[arg]))) {
        std::cout << "-maxT requires a non-zero integer argument." << std::endl;
        argError = true;
      }
    }
    else if (strcmp(argv[arg], "-n") == 0) {
      arg++;
      if (!oneMore || 0 == (fixedTestSizeNum = atoi(argv[arg]))) {
        std::cout << "-t requires a non-zero integer argument." << std::endl;
        argError = true;
      }
      fixedTestSize = true;
    }
    else if (strcmp(argv[arg], "-t") == 0) {
      arg++;
      if (!oneMore || 0 == (sortTestSel = atoi(argv[arg]))) {
        std::cout << "-t requires a non-zero integer argument." << std::endl;
        argError = true;
      }
    }
    else if (strcmp(argv[arg], "-l") == 0) {
      arg++;
      if (!oneMore || 0 == (num_tests = atoi(argv[arg]))) {
        std::cout << "-l requires a non-zero integer argument." << std::endl;
        argError = true;
      }
    }
    else if (strcmp(argv[arg], "-rs") == 0) {
      fixedTestSize = false;
    }
    else if (strcmp(argv[arg], "-dr") == 0) {
      dataType = dtRandom;
    }
    else if (strcmp(argv[arg], "-do") == 0) {
      dataType = dtOrdered;
    }
    else if (strcmp(argv[arg], "-db") == 0) {
      dataType = dtReverseOrdered;
    }
    else if (strcmp(argv[arg], "-nv") == 0) {
      verifySort = false;
    }
    else if (strcmp(argv[arg], "-v") == 0) {
      verifySort = true;
    }
    else if (strcmp(argv[arg], "-h") == 0) {
      printHelp();
      return(0);
    }
    else {
      std::cout << "Argument " << argv[arg] << " not recognized" << std::endl;
      printHelp();
      argError = true;
    }
  }
  if (argError) {
    return 1;
  }

  // print out the data type part of the output header
  switch (dataType) {
  case dtRandom: {
    std::cout << "Random data: "; break;
  }
  case dtOrdered: {
    std::cout << "Ordered data: "; break;
  }
  case dtReverseOrdered: {
    std::cout << "ReverseOrdered data: "; break;
  }
  default: {
    std::cout << "No such data type: " << dataType << std::endl;
    exit(1);
  }
  }

  // print out the test case part of the program header and set the test case.
  SortCase* sortCase = nullptr;
  switch (sortTestSel) {
  case 1: {
    std::cout << "Sort Test Case " << sortTestSel << ", array default direction" << std::endl;
    sortCase = (SortCase*)new arraySortCase();
    break;
  }
  case 2: {
    std::cout << "Sort Test Case " << sortTestSel << ", vector largest to smallest" << std::endl;
    sortCase = (SortCase*)new vectorSortCase();
    break;
  }
  case 3: {
    std::cout << "Sort Test Case " << sortTestSel << ", pointers to strings smallest to largest" << std::endl;
    sortCase = (SortCase*)new textPointerSortCase();
    break;
  }
  default: {
    std::cout << "No such test case: " << sortTestSel << std::endl;
    exit(1);
  }
  }


  // set up the random number ranges for test size.
  auto riSize = RandomInterval<int64_t>(1024LL, 1048576LL, 1);

  // set up a vector of number of treads to test and fill with a range of numbers.
  std::vector<size_t> threadList;
  for (size_t i = minThreads; i <= maxThreads; i++) threadList.push_back(i);

  // set up a counter for number of test tht failed.
  size_t testsFailed = 0;

  // start looping through the vector of the number of threads.
  for (size_t tc = 0; tc < threadList.size(); tc++) {
    size_t threads = threadList[tc];

    // loop through the number of tests to run for each thread count.
    for (size_t test_num = 0; test_num < num_tests; test_num++) {
      int64_t test_size;
      if (fixedTestSize)  // set up either a fixed or random test size. 
        test_size = fixedTestSizeNum;
      else
        test_size = riSize();

      // Generate the test data but only once if it's a fixed data size
      if ((test_num == 0 && tc == 0) || !fixedTestSize) {
        sortCase->generateData(test_size, dataType);
      }

      // run the test case
      double testTime = sortCase->runSort(test_size, threads);

      // Print the execution time.
      std::cout << "Total sort time of " << std::setw(8) << test_size << " elements ";
      std::cout << "using " << threads << " threads = ";
      std::cout << std::fixed << std::setprecision(3) << testTime << " seconds" << std::endl;

      /// And verify the data if requested
      if (verifySort) {
        if (sortCase->verifySort(test_size)) testsFailed++;
      }
    }
  }
  std::cout << "Completed " << num_tests * threadList.size() << " tests";
  if (verifySort) std::cout << " with " << testsFailed << " test failures." << std::endl;
  else std::cout << "." << std::endl;

  sortCase->cleanup();
}

#endif

