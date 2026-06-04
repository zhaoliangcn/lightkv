#pragma once

#include "options.h"
#include "status.h"
#include <string>

namespace lightkv {

// Load Options from a JSON configuration file
// Supports all Options fields with sensible defaults
Status LoadOptionsFromFile(const std::string& config_path, Options* options);

} // namespace lightkv
