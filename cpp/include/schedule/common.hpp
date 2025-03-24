#pragma once
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>

/** Time is measured in seconds
 */
typedef uint_fast64_t Time;
/** Time is measured in seconds
 */
typedef int_fast64_t TimeDelta;

typedef uint_fast64_t Terminal;
typedef uint_fast64_t Cargo;
typedef uint_fast64_t Truck;

/** A class representing a contiguous stretch of time and additional data
 */
template <class T> class IntervalWithData {
private:
  Time m_start_time;
  Time m_end_time;
  T m_additional_data;

public:
  /** Create a new interval.
   * @throws if its length is non-positive
   */
  IntervalWithData(Time t_start_time, Time t_end_time, T t_additional_data);
  Time get_start_time() const;
  Time get_end_time() const;
  TimeDelta get_duration() const;
  T get_additional_data() const;

  /** Return a copy of the interval with changed start, end
   * If the resulting interval is invalid, the result is nullopt_t
   */
  std::optional<IntervalWithData<T>> reschedule(TimeDelta t_start_change,
                                                TimeDelta t_end_change);

  bool operator==(const IntervalWithData<T> &b) const;
};

struct NoData {};
typedef IntervalWithData<NoData> Interval;

/** A chain of intervals that are sorted in-order and are not overlapping
 */
template <class T> class IntervalWithDataChain {
  std::list<IntervalWithData<T>> m_intervals;

public:
  IntervalWithDataChain();
  IntervalWithDataChain(const std::list<IntervalWithData<T>> &data);
  IntervalWithDataChain(const IntervalWithData<T> &interval);

  /** Create an IntervalChain that is the intersection of
   * two IntervalChains, that is sub-intervals occurring in both
   */
  template <class U>
  IntervalWithDataChain<T>
  intersect(const IntervalWithDataChain<U> &other) const;

  /**
   * Checks whether all the intervals in this chain are contained in `other`
   */
  template <class U> bool contained_in(const IntervalWithData<U> &other) const;

  /**
   * Returns the interval chain representing intervals which occur in `other`,
   * but not in `this`. Requires to be contained_in other
   * Returns an interval chain with data being a pair of
   * (possibly empty) references to the interval before and after
   * @throws if this->contained_in(other) is false
   */
  template <class U>
  IntervalWithDataChain<std::pair<std::optional<IntervalWithData<T> &>,
                                  std::optional<IntervalWithData<T> &>>>
  remove_from(const IntervalWithData<U> &other) const;

  /** Remove interval at index `i`.
   * Returns true if and only if this was successful (i.e. this element exists)
   */
  bool erase(unsigned i);

  // Adding elements
  /** Try to push an element to the back of the schedule.
   * If it doesn't belong there (e.g. because it's out of order there),
   * return false and fail; otherwise, succeed and return true.
   */
  bool try_push_back(const IntervalWithData<T> &other);

  std::size_t size() const;
  // Allow iterating over the intervals
  typename std::list<IntervalWithData<T>>::iterator begin();
  typename std::list<IntervalWithData<T>>::iterator end();
};

typedef IntervalWithDataChain<NoData> IntervalChain;

/**
 * Given intervals i1, i2, says that i1 < i2 if and only if
 * end time of i1 is strictly less than start time of i2
 */
template <class T> struct CompareIntervalIntersections {
  bool operator()(const IntervalWithData<T> &i1, const IntervalWithData<T> &i2);
};

/** A class representing information for a transition other than time
 */
class TransitionInfo {
  Terminal m_from;
  Terminal m_to;
  Cargo m_cargo;

public:
  TransitionInfo(Terminal t_from, Terminal t_to, Cargo t_cargo);
};

/** A class representing a truck moving from one terminal to another
 */
typedef IntervalWithData<TransitionInfo> Transition;
typedef IntervalWithDataChain<TransitionInfo> TransitionChain;

/**
 * Generates a random integer in interval [0, range)
 * From https://en.cppreference.com/w/cpp/numeric/random/rand
 */
unsigned bounded_rand(unsigned range);
