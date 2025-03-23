#include "common.hpp"
#include <algorithm>
#include <cassert>

template <class T> Time IntervalWithData<T>::get_start_time() const {
  return this->m_start_time;
}
template <class T> Time IntervalWithData<T>::get_end_time() const {
  return this->m_start_time + this->m_duration;
}

template <class T>
IntervalWithData<T>::IntervalWithData(Time t_start_time, Time t_end_time,
                                      T t_additional_data)
    : m_start_time(t_start_time), m_additional_data(t_additional_data) {
  assert(t_end_time > t_start_time);
  this->m_duration = t_end_time - t_start_time;
}

IntervalWithData<void>::IntervalWithData(Time t_start_time, Time t_end_time)
    : m_start_time(t_start_time) {
  assert(t_end_time > t_start_time);
  this->m_duration = t_end_time - t_start_time;
}

template <class T> Interval IntervalWithData<T>::remove_data() const {
  return Interval(this->m_start_time, this->m_duration);
}

Interval Interval::intersect(const Interval &other) const {
  return Interval(std::max(this->m_start_time, other.m_start_time),
                  std::min(this->get_end_time(), other.get_end_time()));
}

template <class T>
IntervalWithData<T>
IntervalWithData<T>::intersect(const Interval &other) const {
  Interval interval = this->remove_data().intersect(other);
  return IntervalWithData<T>(interval.get_start_time(), interval.get_end_time(),
                             this->m_additional_data);
}

template <class T>
IntervalChain<T> IntervalChain<T>::intersect(const Interval &other) const {
  Time other_start_time = other.get_start_time();
  Time other_end_time = other.get_end_time();

  // Copy over only intervals we are interested in
  IntervalChain<T> out = IntervalChain<T>();
  for (IntervalWithData<T> this_interval : this->m_intervals) {
    // If not yet in range, keep going
    Time this_end_time = this_interval.get_end_time();
    if (this_end_time <= other_start_time) {
      continue;
    }

    // If out of range, we are done
    Time this_start_time = this_interval.get_start_time();
    if (this_start_time >= other_end_time) {
      return out;
    }

    // Add the interval
    out.m_intervals.push_back(this->intersect(other));
  }
  return out;
}

template <class T>
bool CompareIntervalIntersections<T>::operator()(
    const IntervalWithData<T> &i1, const IntervalWithData<T> &i2) {
  return true;
}
bool Interval::operator==(const Interval &b) const {
  return (this->m_start_time == b.m_start_time) &&
         (this->m_duration == b.m_start_time);
}
