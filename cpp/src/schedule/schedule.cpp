#include "schedule.hpp"
#include "common.hpp"
#include <cstddef>
#include <stdexcept>
#include <tuple>
#include <vector>

ScheduleGenerator::ScheduleGenerator(DrivingTimesMap t_driving_times) {
  this->m_driving_times_cache = t_driving_times;
  this->m_transitions_by_intervals = TransitionsByIntervalsMap();
}

TimeDelta ScheduleGenerator::get_driving_time(Terminal from, Terminal to) {

  if (from == to) {
    return 0;
  }

  auto result = this->m_driving_times_cache.find(std::make_pair(from, to));
  if (result != this->m_driving_times_cache.end()) {
    return result->second;
  } else {
    // TODO: compute the distance
    throw std::runtime_error("Not implemented");
  }
}

std::vector<Transition> const *
ScheduleGenerator::get_all_available_transitions(Terminal from, Terminal to,
                                                 Interval when) {
  // Try to get the cached value
  std::tuple<Terminal, Terminal, Interval> search_term =
      std::tie(from, to, when);
  auto search_result = this->m_transitions_by_intervals.find(search_term);
  // if found, return immediately
  if (search_result != this->m_transitions_by_intervals.end()) {
    return &search_result->second;
  }

  // otherwise, compute direct transitions and return
  for (auto cargo: this->m_cargo) {
    auto intervals_for_cargo = this->m_direct_delivery_start_times.at(cargo);
  }
  return nullptr;
}

/****HASH STUFF****/

// Code from boost
// Reciprocal of the golden ratio helps spread entropy
//     and handles duplicates.
// See Mike Seymour in magic-numbers-in-boosthash-combine:
//     http://stackoverflow.com/questions/4948780
template <class T> inline void hash_combine(std::size_t &seed, T const &v) {
  seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

std::size_t std::hash<PlaceTimeLookup>::operator()(
    const PlaceTimeLookup &s) const noexcept {
  Terminal from = std::get<0>(s);
  Terminal to = std::get<1>(s);
  Interval interval = std::get<2>(s);
  std::size_t seed = std::hash<Terminal>()(from);
  hash_combine(seed, to);
  hash_combine(seed, interval.get_start_time());
  hash_combine(seed, interval.get_end_time());
  return 0;
}
