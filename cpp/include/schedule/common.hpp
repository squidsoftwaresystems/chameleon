#pragma once
#include <cstdint>
#include <list>

/** Time is measured in seconds
 */
typedef uint_fast64_t Time;
/** Time is measured in seconds
 */
typedef uint_fast64_t TimeDelta;

typedef uint_fast64_t Terminal;
typedef uint_fast64_t Cargo;

template <class T> class IntervalWithData;
typedef IntervalWithData<void> Interval;
/** A class representing a contiguous stretch of time and additional data
 */
template <class T> class IntervalWithData {
  Time m_start_time;
  TimeDelta m_duration;
  T m_additional_data;

public:
  IntervalWithData(Time t_start_time, Time t_end_time, T t_additional_data);
  Time get_start_time() const;
  Time get_end_time() const;
  /**
   * Return a copy of this interval, intersected with a different
   * interval
   * Throws an exception if interval has non-positive length
   */
  IntervalWithData<T> intersect(const Interval &other) const;

  /**
   * Remove data and make it a normal Interval
   */
  Interval remove_data() const;
};

/** A class representing a contiguous stretch of time and no additional data
 */
template <> class IntervalWithData<void> {
  Time m_start_time;
  TimeDelta m_duration;

public:
  IntervalWithData(Time t_start_time, Time t_end_time);
  Time get_start_time() const;
  Time get_end_time() const;
  Interval remove_data() const;
  /**
   * Return a copy of this interval, intersected with a different
   * interval
   * Throws an exception if interval has non-positive length
   */
  Interval intersect(const Interval &other) const;

  bool operator==(const Interval &b) const;
};

/** A chain of intervals that are sorted in-order and are not overlapping
 */
template <class T> class IntervalChain {
  std::list<IntervalWithData<T>> m_intervals;

  /** Create an IntervalChain that is the intersection of this
   * IntervalChain and the given interval
   */

  IntervalChain<T> intersect(const Interval &other) const;
};

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
};

/** A class representing a truck moving from one terminal to another
 */
typedef IntervalWithData<TransitionInfo> Transition;
