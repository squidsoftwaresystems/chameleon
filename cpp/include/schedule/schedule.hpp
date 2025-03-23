#pragma once
#include "common.hpp"
#include <map>
#include <tuple>
#include <unordered_map>
#include <vector>

/** Map of cargo to a chain of intervals
 */
typedef std::map<Cargo, IntervalChain<void>> IntervalChainForCargo;
typedef std::tuple<Terminal, Terminal, Interval> PlaceTimeLookup;
/**
 * Specify how to hash PlaceTimeLookup
 */
template <> struct std::hash<PlaceTimeLookup> {
  std::size_t operator()(const PlaceTimeLookup &lookup) const noexcept;
};

typedef std::map<std::pair<Terminal, Terminal>, TimeDelta> DrivingTimesMap;
typedef std::unordered_map<PlaceTimeLookup, std::vector<Transition>>
    TransitionsByIntervalsMap;

/** A class that is used to generate schedules
 */
class ScheduleGenerator {
  /**
   * Intervals when the terminals are open
   */
  std::map<Terminal, Interval> m_terminal_open_intervals;

  /** A map from (from_terminal, to_terminal) to cached driving times
   */
  DrivingTimesMap m_driving_times_cache;
  TransitionsByIntervalsMap m_transitions_by_intervals;

  /**
   * Times during which pickup can occur. Takes into account e.g. terminals
   * closing overnight
   */
  IntervalChainForCargo m_pickup_times;

  /**
   * Times during which dropoff can occur. Takes into account e.g. terminals
   * closing overnight
   */
  IntervalChainForCargo m_dropoff_times;

  /**
   * Times during which a pickup can occur and a truck can drive directly to
   * destination and be on time for drop-off. Takes into account e.g. terminals
   * closing overnight
   */
  IntervalChainForCargo m_direct_delivery_start_times;

  std::vector<Cargo> m_cargo;

public:
  ScheduleGenerator(DrivingTimesMap t_driving_times);
  TimeDelta get_driving_time(Terminal from, Terminal to);

  /**
   * Gets all transitions which can be done starting at terminal `from`, ending
   * at `to`, and driving to transition start terminal, driving through the
   * transition, and then driving to `to`. Does not check whether this cargo has
   * been delivered or not
   */
  std::vector<Transition> const *
  get_all_available_transitions(Terminal from, Terminal to, Interval when);
};
