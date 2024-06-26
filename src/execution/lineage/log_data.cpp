#ifdef LINEAGE
#include "duckdb/execution/lineage/log_data.hpp"

namespace duckdb {

  std::pair<int, int> Log::LatestLSN() {
    return latest;
  }

  void Log::SetLatestLSN(std::pair<int, int> lsn_fn) {
    latest = lsn_fn;
  }

} // namespace duckdb
#endif
