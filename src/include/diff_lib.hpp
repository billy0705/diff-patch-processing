#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace diffpatch {

// Generate a compact patch (JSON array of ops) transforming old -> neu.
// Ops are arrays: ["=", n] | ["-", n] | ["+", string]
// Counts are in UTF-8 code points.
std::string GeneratePatchJson(const std::string &old_utf8, const std::string &neu_utf8, bool autojunk = false,
                              const std::function<bool(uint32_t)> &isjunk = nullptr);

} // namespace diffpatch
