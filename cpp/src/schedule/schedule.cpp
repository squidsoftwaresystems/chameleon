#include "schedule.hpp"
#include "common.hpp"
#include <cassert>
#include <cstddef>
#include <stdexcept>
#include <strings.h>
#include <tuple>
#include <vector>

Schedule::Schedule(std::map<Truck, TransitionChain> t_truck_transitions) {
  this->m_truck_transitions = t_truck_transitions;
}

ScheduleGenerator::ScheduleGenerator(DrivingTimesMap t_driving_times,
                                     Interval t_planning_period)
    : m_driving_times_cache(t_driving_times),
      m_planning_period(t_planning_period) {
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

void ScheduleGenerator::append_all_possible_transitions_for_window(
    Terminal from, Terminal to, Interval when,
    std::vector<Transition> &transitions) {
  // Try to get the cached value
  std::tuple<Terminal, Terminal, Interval> search_term =
      std::tie(from, to, when);
  auto search_result = this->m_transitions_by_intervals.find(search_term);
  // if found, return immediately
  if (search_result != this->m_transitions_by_intervals.end()) {
    // Append this vector onto the given vector
    transitions.insert(transitions.end(), search_result->second.begin(),
                       search_result->second.end());
    return;
  }

  std::vector<Transition> out = std::vector<Transition>();

  // otherwise, compute direct transitions and return
  for (auto cargo_and_delivery_start_times :
       this->m_direct_delivery_start_times) {
    Cargo cargo = cargo_and_delivery_start_times.first;
    CargoDeliveryInformation cargo_delivery_info =
        cargo_and_delivery_start_times.second;

    // Driving from current terminal to start terminal of cargo
    TimeDelta driving_time1 =
        this->get_driving_time(from, cargo_delivery_info.m_from);
    // Driving from terminal where cargo is dropped of to end terminal
    TimeDelta driving_time2 =
        this->get_driving_time(cargo_delivery_info.m_to, to);
    // Time to drive between the two terminals, delivering this cargo
    TimeDelta delivery_duration = cargo_delivery_info.m_direct_driving_time;

    // When can we start the delivery so that we can get to the starting
    // terminal, do the delivery, and return, all within `when` interval
    auto padded_delivery_start_times =
        when.reschedule(driving_time1, -driving_time2 - delivery_duration);

    // If there is enough time to do this at all
    if (padded_delivery_start_times) {
      IntervalChain allowed_windows =
          cargo_delivery_info.m_direct_delivery_start_times.intersect(
              IntervalChain(*padded_delivery_start_times));

      // Otherwise, add a few options for when the delivery can be started.
      for (auto window : allowed_windows) {
        Time window_start = window.get_start_time();
        Time window_end = window.get_end_time();
        TransitionInfo transition_info = TransitionInfo(
            cargo_delivery_info.m_from, cargo_delivery_info.m_to, cargo);
        // 1. As soon as we are free
        transitions.push_back(Transition(
            window_start, window_start + delivery_duration, transition_info));

        // 2. At last possible moment
        transitions.push_back(Transition(window_end - delivery_duration,
                                         window_end, transition_info));
      }
    }
  }
  return;
}

std::optional<Schedule>
ScheduleGenerator::get_schedule_neighbour(const Schedule &original,
                                          uint_fast64_t num_tries) {
  for (int try_index = 0; try_index < num_tries; try_index++) {
    // Pick a truck randomly
    Truck truck = this->m_trucks.at(bounded_rand(this->m_trucks.size()));
    TransitionChain transitions = original.m_truck_transitions.at(truck);

    // We should only plan within the specified plan period
    assert(transitions.contained_in(this->m_planning_period));

    auto gaps = transitions.remove_from(this->m_planning_period);

    // Decide at random which transition or interval between transitions to use
    unsigned num_transitions = transitions.size();
    unsigned num_gaps = gaps.size();

    unsigned index = bounded_rand(num_transitions + num_gaps);

    // If we should remove a transition
    if (index < num_transitions) {
      // Copy over the map
      // TODO: check this works proprely
      std::map<Truck, TransitionChain> truck_transitions(
          original.m_truck_transitions);
      // Remove `index`th transition
      truck_transitions[truck].erase(index);
    } else {
      // If we should add a transition
      unsigned gap_index = index - num_transitions;
      gaps.at(gap_index);
    }
  }
  // We failed, return nothing
  return {};
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
