#pragma once
#include "common.hpp"
#include <cstddef>
#include <cstdint>
#include <list>
#include <map>



class TruckPlan {
  std::list<Transition> m_transitions;
public:
  TruckPlan();
};

