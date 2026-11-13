#pragma once

#include "sysbench/db.hpp"

namespace sb::mysql {

// Registers the MySQL driver in the process-wide protocol registry.  Safe to
// call more than once.
void register_driver();

}  // namespace sb::mysql
