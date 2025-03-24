#pragma once
#include "common.hpp"
#include <cstdint>
#include <map>
#include <tuple>
#include <unordered_map>
#include <vector>

/** A key to look up by place and time
 */
typedef std::tuple<Terminal, Terminal, Interval> PlaceTimeLookup;

/** Main information needed to schedule delivery of particular cargo
 */
struct CargoDeliveryInformation {
  /**
   * Times during which a pickup can occur and a truck has enough time to drive
   * directly to destination and be on time for drop-off. Takes into account
   * e.g. terminals closing overnight
   */
  IntervalChain m_direct_delivery_start_times;
  TimeDelta m_direct_driving_time;

  /** Terminal where cargo can be picked up from
   */
  Terminal m_from;
  /** Terminal where cargo needs to be dropped off to
   */
  Terminal m_to;
};

/**
 * Specify how to hash PlaceTimeLookup
 */
template <> struct std::hash<PlaceTimeLookup> {
  std::size_t operator()(const PlaceTimeLookup &lookup) const noexcept;
};

typedef std::map<std::pair<Terminal, Terminal>, TimeDelta> DrivingTimesMap;
typedef std::unordered_map<PlaceTimeLookup, std::vector<Transition>>
    TransitionsByIntervalsMap;

class Schedule {
  // ScheduleGenerator should be able to access internal data of a schedule
  friend class ScheduleGenerator;
  /** A map from trucks to transitions
   */
  std::map<Truck, TransitionChain> m_truck_transitions;

public:
  Schedule(std::map<Truck, TransitionChain> t_truck_transitions);
};

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
  std::map<Cargo, IntervalChain> m_pickup_times;

  /**
   * Times during which dropoff can occur. Takes into account e.g. terminals
   * closing overnight
   */
  std::map<Cargo, IntervalChain> m_dropoff_times;

  std::map<Cargo, CargoDeliveryInformation> m_direct_delivery_start_times;

  std::vector<Truck> m_trucks;

  Interval m_planning_period;

public:
  ScheduleGenerator(DrivingTimesMap t_driving_times, Interval t_planning_period);
  TimeDelta get_driving_time(Terminal from, Terminal to);

  /**
   * Appends to a vector transitions which can be done starting at terminal
   * `from`, ending at `to`, and driving to transition start terminal, driving
   * through the transition, and then driving to `to`. Does not check whether
   * this cargo has been delivered or not
   */
  void append_all_possible_transitions_for_window(
      Terminal from, Terminal to, Interval when,
      std::vector<Transition> &transitions);

  /** Gets a random neighbour for a schedule.
   * Note that the neighbours might not be sampled uniformly
   * @param num_tries number of times that we try to generate a truck for before
   * giving up. This helps protect against e.g. schedules that have no
   * neighbours
   */
  // TODO: check if the schedule does indeed have no neighbours
  std::optional<Schedule> get_schedule_neighbour(const Schedule &original,
                                                 uint_fast64_t num_tries);
};
