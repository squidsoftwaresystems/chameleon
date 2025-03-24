#include "common.hpp"
#include <algorithm>
#include <cassert>
#include <list>
#include <optional>

template <class T> Time IntervalWithData<T>::get_start_time() const {
  return this->m_start_time;
}
template <class T> Time IntervalWithData<T>::get_end_time() const {
  return this->m_end_time;
}
template <class T> TimeDelta IntervalWithData<T>::get_duration() const {
  return this->m_end_time - this->m_start_time;
}

template <class T> T IntervalWithData<T>::get_additional_data() const {
  return this->m_additional_data;
}

template <class T>
std::optional<IntervalWithData<T>>
IntervalWithData<T>::reschedule(TimeDelta t_start_change,
                                TimeDelta t_end_change) {
  // TODO: assert that there is no overflow
  Time start_time = this->m_start_time + t_start_change;
  Time end_time = this->m_end_time + t_end_change;
  if (end_time <= start_time)
    return {};
  else
    return IntervalWithData<T>(start_time, end_time, this->m_additional_data);
}

template <class T>
IntervalWithData<T>::IntervalWithData(Time t_start_time, Time t_end_time,
                                      T t_additional_data) {
  assert(t_end_time > t_start_time);
  this->m_start_time = t_start_time;
  this->m_end_time = t_end_time;
  this->m_additional_data = t_additional_data;
}

template <class T>
IntervalWithDataChain<T>::IntervalWithDataChain(
    const std::list<IntervalWithData<T>> &data)
    : m_intervals(data) {
  // Check that it is in an increasing order and non-overlapping
  // HACK: this technically doesn't allow intervals
  // to start at 0
  Time last_end = 0;
  for (auto interval : data) {
    assert(last_end < interval->get_start_time());
    last_end = interval->get_end_time();
  }
}

template <class T>
IntervalWithDataChain<T>::IntervalWithDataChain(
    const IntervalWithData<T> &interval) {
  this->m_intervals = std::list<IntervalWithData<T>>(interval);
}

template <class T>
template <class U>
IntervalWithDataChain<T> IntervalWithDataChain<T>::intersect(
    const IntervalWithDataChain<U> &other) const {
  // Lock-step with `other`, adding intervals if they intersect
  IntervalWithDataChain<T> out = IntervalWithDataChain<T>();

  // Take iterators
  auto this_it = this->m_intervals.begin();
  auto other_it = other->m_intervals.begin();

  // While we have intervals left over in both
  while (this_it != this->m_intervals.end() &&
         other_it != other.m_intervals.end()) {
    Time this_start_time = this_it->get_start_time();
    Time this_end_time = this_it->get_end_time();
    Time other_start_time = other_it->get_start_time();
    Time other_end_time = other_it->get_end_time();

    // Add the intersection if they intersect
    if (other_end_time > this_start_time && this_end_time > other_start_time) {
      Time start_time = std::max(this_start_time, other_start_time);
      Time end_time = std::max(this_end_time, other_end_time);

      out.m_intervals.push_back(IntervalWithData<T>(
          start_time, end_time, this_it->m_additional_data));
    }

    // advance the iterator that has earlier end_time
    if (this_end_time < other_end_time) {
      ++this_it;
    } else {
      ++other_it;
    }
  }

  // Add the interval
  return out;
}

template <class T>
template <class U>
bool IntervalWithDataChain<T>::contained_in(
    const IntervalWithData<U> &other) const {
  if (this->m_intervals.empty())
    return true;
  return (other.get_start_time() <=
          this->m_intervals.front()->get_start_time()) &&
         (this->m_intervals.back().get_end_time() <= other.get_end_time());
}

template <class T>
template <class U>
IntervalWithDataChain<std::pair<std::optional<IntervalWithData<T> &>,
                                std::optional<IntervalWithData<T> &>>>
IntervalWithDataChain<T>::remove_from(const IntervalWithData<U> &other) const {
  assert(this->contained_in(other));

  std::list out = std::list<std::pair<std::optional<IntervalWithData<T> &>,
                                      std::optional<IntervalWithData<T> &>>>();

  Time start_time = other.get_start_time();
  std::optional<IntervalWithData<T>> previous_interval = std::nullopt;

  for (auto interval : this->m_intervals) {
    Time this_start_time = interval->get_start_time();
    // Do not add empty intervals
    if (start_time < this_start_time) {
      out.push_back(Interval(start_time, this_start_time,
                             {previous_interval, {interval}}));
    }
    start_time = interval->get_end_time();
    previous_interval = {interval};
  }

  // `this->m_intervals` could end before `other` does
  if (start_time < other.get_end_time()) {
    out.push_back(Interval(start_time, other.get_end_time(),
                           {previous_interval, std::nullopt}));
  }

  return IntervalWithDataChain(out);
}

template <class T> bool IntervalWithDataChain<T>::erase(unsigned i) {
  if (i >= this->m_intervals.size()) {
    return false;
  } else {
    // delete ith element
    this->m_intervals.erase(this->m_intervals.begin() + i);
    return true;
  }
}

template <class T>
bool IntervalWithDataChain<T>::try_push_back(const IntervalWithData<T> &other) {
  Time start_time = other.get_start_time();
  // If not empty and last event ends before this one starts, then fail
  if (!this->m_intervals.empty() &&
      this->m_intervals.back().get_end_time() >= start_time)
    return false;

  this->m_intervals.push_back(other);
  return true;
}

template <class T> std::size_t IntervalWithDataChain<T>::size() const {
  return this->m_intervals.size();
}

template <class T>
typename std::list<IntervalWithData<T>>::iterator
IntervalWithDataChain<T>::begin() {
  return this->m_intervals.begin();
}

template <class T>
typename std::list<IntervalWithData<T>>::iterator
IntervalWithDataChain<T>::end() {
  return this->m_intervals.end();
}

template <class T>
bool CompareIntervalIntersections<T>::operator()(
    const IntervalWithData<T> &i1, const IntervalWithData<T> &i2) {
  return true;
}
template <class T>
bool IntervalWithData<T>::operator==(const IntervalWithData<T> &b) const {
  return (this->m_start_time == b.m_start_time) &&
         (this->m_end_time == b.m_end_time);
}

TransitionInfo::TransitionInfo(Terminal t_from, Terminal t_to, Cargo t_cargo)
    : m_from(t_from), m_to(t_to), m_cargo(t_cargo) {}

unsigned bounded_rand(unsigned range) {
  for (unsigned x, r;;)
    if (x = rand(), r = x % range, x - r <= -range)
      return r;
}
